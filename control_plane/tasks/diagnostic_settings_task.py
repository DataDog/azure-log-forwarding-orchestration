# stdlib
from asyncio import gather
import asyncio
from copy import deepcopy
from json import dumps
from logging import ERROR, INFO, basicConfig, getLogger
from typing import AsyncIterable, TypeVar
from uuid import uuid4

# 3p
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.mgmt.monitor.v2021_05_01_preview.aio import MonitorManagementClient
from azure.mgmt.monitor.v2021_05_01_preview.models import (
    DiagnosticSettingsResource,
    LogSettings,
    CategoryType,
    Resource,
)

# project
from cache.common import InvalidCacheError, read_cache, write_cache
from cache.diagnostic_settings_cache import (
    DIAGNOSTIC_SETTINGS_CACHE_BLOB,
    DiagnosticSettingConfiguration,
    deserialize_diagnostic_settings_cache,
)
from cache.resources_cache import RESOURCE_CACHE_BLOB, deserialize_resource_cache
from tasks.task import Task, now


# silence azure logging except for errors
getLogger("azure").setLevel(ERROR)


DIAGNOSTIC_SETTINGS_TASK_NAME = "diagnostic_settings_task"
EVENT_HUB_NAME_SETTING = "EVENT_HUB_NAME"
EVENT_HUB_NAMESPACE_SETTING = "EVENT_HUB_NAMESPACE"
DIAGNOSTIC_SETTING_PREFIX = "datadog_log_forwarding_"

log = getLogger(DIAGNOSTIC_SETTINGS_TASK_NAME)


DiagnosticSettingType = TypeVar("DiagnosticSettingType", bound=Resource)


async def get_existing_diagnostic_setting(
    resource_id: str,
    settings: AsyncIterable[DiagnosticSettingType],
    existing_diagnostic_setting_name: str | None = None,
) -> DiagnosticSettingType | None:
    try:
        async for s in settings:
            if (existing_diagnostic_setting_name is not None and s.name == existing_diagnostic_setting_name) or (
                existing_diagnostic_setting_name is None and s.name.startswith(DIAGNOSTIC_SETTING_PREFIX)  # type: ignore
            ):
                return s
        log.debug("No existing diagnostic setting found for resource %s", resource_id)
        return None
    except ResourceNotFoundError as e:
        log.warning("Resource %s not found: %s", resource_id, e.error)
        raise
    except HttpResponseError as e:
        if e.error and e.error.code == "ResourceTypeNotSupported":
            log.debug("Got ResourceTypeNotSupported error for resource id %s", resource_id)
            raise
        log.error("Failed to get diagnostic settings for %s", resource_id, exc_info=True)
        raise


def diagnostic_setting_name(config: DiagnosticSettingConfiguration) -> str:
    return DIAGNOSTIC_SETTING_PREFIX + config["id"]


class DiagnosticSettingsTask(Task):
    def __init__(self, resource_cache_state: str, diagnostic_settings_cache_state: str) -> None:
        super().__init__()

        # read caches
        success, resource_cache = deserialize_resource_cache(resource_cache_state)
        if not success:
            raise InvalidCacheError("Resource Cache is in an invalid format, failing this task until it is valid")
        self.resource_cache = resource_cache

        success, diagnostic_settings_cache = deserialize_diagnostic_settings_cache(diagnostic_settings_cache_state)
        if not success:
            log.warning("Diagnostic Settings Cache is in an invalid format, resetting the cache")
            diagnostic_settings_cache = {}
        self._diagnostic_settings_cache_initial = diagnostic_settings_cache
        self.diagnostic_settings_cache = deepcopy(self._diagnostic_settings_cache_initial)

    async def run(self) -> None:
        log.info("Crawling %s subscriptions", len(self.resource_cache))
        await gather(
            *[self.process_subscription(sub_id, resources) for sub_id, resources in self.resource_cache.items()]
        )

    async def process_subscription(self, sub_id: str, resources_per_region: dict[str, set[str]]) -> None:
        log.info("Crawling %s regions for subscription %s", len(resources_per_region), sub_id)
        async with MonitorManagementClient(self.credential, sub_id) as client:
            # client.management_group_diagnostic_settings.list("management_group_id") TODO: do we want to do anything with this?
            await gather(
                *(
                    [
                        self.process_region(client, sub_id, region, resource_ids)
                        for region, resource_ids in resources_per_region.items()
                    ]
                    + [self.update_subscription_settings(sub_id, client)]
                )
            )

    async def update_subscription_settings(self, subscription_id: str, client: MonitorManagementClient) -> None:
        if (
            setting := await get_existing_diagnostic_setting(
                subscription_id, client.subscription_diagnostic_settings.list()
            )
        ) and setting.logs:
            # We have already added the setting for this subscription
            ...

    async def process_region(
        self,
        client: MonitorManagementClient,
        sub_id: str,
        region: str,
        resource_ids: set[str],
    ) -> None:
        await gather(*[self.process_resource(client, sub_id, region, resource_id) for resource_id in resource_ids])

    async def process_resource(
        self,
        client: MonitorManagementClient,
        sub_id: str,
        region: str,
        resource_id: str,
    ) -> None:
        if configuration := self.diagnostic_settings_cache.get(sub_id, {}).get(resource_id):
            try:
                existing_setting = await get_existing_diagnostic_setting(
                    resource_id,
                    client.diagnostic_settings.list(resource_id),
                    existing_diagnostic_setting_name=diagnostic_setting_name(configuration),
                )
            except Exception:
                # TODO(AZINTS-2577) Error handling
                return

            if existing_setting:
                # We have already added the setting for this resource
                # do we ever want to update categories or anything?
                # or trust that the customer knows what they are doing if they modify the setting?
                # can use this to find categories: client.diagnostic_settings_category.list(resource["id"])
                pass
            else:
                # The setting has been removed, we should put it back
                log.debug("Re-adding diagnostic setting for resource %s", resource_id)
                await self.add_diagnostic_setting(
                    client,
                    sub_id,
                    resource_id,
                    configuration,
                )
        else:
            # We don't have a configuration for this resource, we should add it
            diagnostic_setting_id = str(uuid4())
            # TODO(AZINTS-2569) determine the appropriate configuration for this resource based on region
            # await self.add_diagnostic_setting(
            #     client, sub_id, resource_id, str(uuid4()), EVENT_HUB_NAME, EVENT_HUB_NAMESPACE
            # )
            self.diagnostic_settings_cache.setdefault(sub_id, {})[resource_id] = {
                "id": diagnostic_setting_id,
                "type": "eventhub",
                "event_hub_name": "TODO",
                "event_hub_namespace": "TODO",
            }

    async def add_diagnostic_setting(
        self,
        client: MonitorManagementClient,
        sub_id: str,
        resource_id: str,
        configuration: DiagnosticSettingConfiguration,
    ) -> None:
        try:
            categories = [
                category
                async for category in client.diagnostic_settings_category.list(resource_id)
                if category.category_type == CategoryType.LOGS
            ]
            if not categories:
                log.debug("No log categories found for resource %s", resource_id)
                return

            log_config = [LogSettings(category=category.name, enabled=True) for category in categories]
            if configuration["type"] == "eventhub":
                resource_group = "lfo"  # TODO(AZINTS-2569): programatically get resource group of the eventhub
                authorization_rule_id = f"/subscriptions/{sub_id}/resourcegroups/{resource_group}/providers/Microsoft.EventHub/namespaces/{configuration['event_hub_namespace']}/authorizationrules/RootManageSharedAccessKey"
                diagnostic_setting = DiagnosticSettingsResource(
                    event_hub_authorization_rule_id=authorization_rule_id,
                    event_hub_name=configuration["event_hub_name"],
                    logs=log_config,
                )
            else:
                diagnostic_setting = DiagnosticSettingsResource(
                    storage_account_id=configuration["storage_account_id"],
                    logs=log_config,
                )
            await client.diagnostic_settings.create_or_update(
                resource_id,
                diagnostic_setting_name(configuration),
                diagnostic_setting,
            )
            log.info("Added diagnostic setting for resource %s", resource_id)
        except HttpResponseError as e:
            if e.error and e.error.code == "ResourceTypeNotSupported":
                # This resource does not support diagnostic settings
                return
            if "Resources should be in the same region" in str(e):
                # todo this should not happen in the real implementation, for now ignore
                return
            if "reused in different settings on the same category for the same resource" in str(e):
                log.error(
                    "Resource %s already has a diagnostic setting with the same configuration: %s",
                    resource_id,
                    configuration,
                )
                return

            log.error("Failed to add diagnostic setting for resource %s -- %s", resource_id, e.error)
        except Exception:
            log.error(
                "Unexpected error when trying to add diagnostic setting for resource %s", resource_id, exc_info=True
            )

    async def write_caches(self) -> None:
        if self.diagnostic_settings_cache == self._diagnostic_settings_cache_initial:
            log.info("Diagnostic settings have not changed, no update needed")
            return
        await write_cache(DIAGNOSTIC_SETTINGS_CACHE_BLOB, dumps(self.diagnostic_settings_cache))
        num_resources = sum(len(resources) for resources in self.diagnostic_settings_cache.values())
        log.info("Updated setting, %s resources stored in the settings cache", num_resources)


async def main():
    basicConfig(level=INFO)
    log.info("Started task at %s", now())
    resources, diagnostic_settings = await gather(
        read_cache(RESOURCE_CACHE_BLOB), read_cache(DIAGNOSTIC_SETTINGS_CACHE_BLOB)
    )
    async with DiagnosticSettingsTask(resources, diagnostic_settings) as task:
        await task.run()
    log.info("Task finished at %s", now())


if __name__ == "__main__":
    asyncio.run(main())
