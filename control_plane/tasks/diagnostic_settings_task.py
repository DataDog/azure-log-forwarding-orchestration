# stdlib
from asyncio import gather, run
from collections.abc import AsyncIterable
from copy import deepcopy
from json import dumps
from logging import ERROR, INFO, basicConfig, getLogger
from typing import NamedTuple, TypeVar

# 3p
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.mgmt.monitor.v2021_05_01_preview.aio import MonitorManagementClient
from azure.mgmt.monitor.v2021_05_01_preview.models import (
    CategoryType,
    DiagnosticSettingsResource,
    LogSettings,
    Resource,
)

from cache.assignment_cache import ASSIGNMENT_CACHE_BLOB, deserialize_assignment_cache

# project
from cache.common import (
    InvalidCacheError,
    LogForwarderType,
    get_config_option,
    get_event_hub_name,
    get_event_hub_namespace,
    get_resource_group_id,
    get_storage_account_id,
    read_cache,
    write_cache,
)
from cache.diagnostic_settings_cache import (
    DIAGNOSTIC_SETTINGS_CACHE_BLOB,
    deserialize_diagnostic_settings_cache,
)
from tasks.common import now
from tasks.task import Task

# silence azure logging except for errors
getLogger("azure").setLevel(ERROR)

DIAGNOSTIC_SETTINGS_TASK_NAME = "diagnostic_settings_task"
log = getLogger(DIAGNOSTIC_SETTINGS_TASK_NAME)


DIAGNOSTIC_SETTING_PREFIX = "datadog_log_forwarding_"


def get_diagnostic_setting_name(config_id: str) -> str:
    return DIAGNOSTIC_SETTING_PREFIX + config_id


def get_authorization_rule_id(sub_id: str, resource_group: str, config_id: str) -> str:  # pragma: no cover
    # no test coverage because we don't have event hub support yet
    return (
        get_resource_group_id(sub_id, resource_group)
        + "/providers/Microsoft.EventHub/namespaces/"
        + get_event_hub_namespace(config_id)
        + "/authorizationrules/RootManageSharedAccessKey"
    )


class DiagnosticSettingConfiguration(NamedTuple):
    "Convenience Tuple for holding the configuration of a diagnostic setting"

    id: str
    type: LogForwarderType


def get_diagnostic_setting(
    sub_id: str, resource_group: str, config: DiagnosticSettingConfiguration, categories: list[str]
) -> DiagnosticSettingsResource:
    log_settings = [LogSettings(category=category, enabled=True) for category in categories]
    if config.type == "eventhub":
        return DiagnosticSettingsResource(  # pragma: no cover
            event_hub_authorization_rule_id=get_authorization_rule_id(sub_id, resource_group, config.id),
            event_hub_name=get_event_hub_name(config.id),
            logs=log_settings,
        )
    else:
        return DiagnosticSettingsResource(
            storage_account_id=get_storage_account_id(sub_id, resource_group, config.id),
            logs=log_settings,
        )


DiagnosticSetting = TypeVar("DiagnosticSetting", bound=Resource)


async def get_existing_diagnostic_setting(
    resource_id: str,
    settings: AsyncIterable[DiagnosticSetting],
    existing_diagnostic_setting_name: str | None = None,
) -> DiagnosticSetting | None:
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


class DiagnosticSettingsTask(Task):
    def __init__(self, assignment_cache_state: str, diagnostic_settings_cache_state: str, resource_group: str) -> None:
        super().__init__()

        # read caches
        success, assignment_cache = deserialize_assignment_cache(assignment_cache_state)
        if not success:
            raise InvalidCacheError("Assignment Cache is in an invalid format, failing this task until it is valid")
        self.assignment_cache = assignment_cache

        success, diagnostic_settings_cache = deserialize_diagnostic_settings_cache(diagnostic_settings_cache_state)
        if not success:
            log.warning("Diagnostic Settings Cache is in an invalid format, resetting the cache")
            diagnostic_settings_cache = {}
        self._diagnostic_settings_cache_initial = diagnostic_settings_cache
        self.diagnostic_settings_cache = deepcopy(self._diagnostic_settings_cache_initial)

        self.resource_group = resource_group

    async def run(self) -> None:
        subs = set(self.assignment_cache) | set(self.diagnostic_settings_cache)
        log.info("Processing %s subscriptions", len(subs))

        await gather(*map(self.process_subscription, subs))

    async def process_subscription(self, sub_id: str) -> None:
        log.info("Processing subscription %s", sub_id)
        # we assume no resources need to be "cleaned up"
        # because if they aren't in the assignment cache then they have been deleted
        async with MonitorManagementClient(self.credential, sub_id) as client:
            # TODO: do we want to do anything with management group diagnostic settings?
            # client.management_group_diagnostic_settings.list("management_group_id")

            await gather(
                *(
                    [
                        self.process_resource(
                            client,
                            sub_id,
                            resource,
                            DiagnosticSettingConfiguration(config_id, config["configurations"][config_id]),
                        )
                        for config in self.assignment_cache[sub_id].values()
                        for resource, config_id in config["resources"].items()
                    ]
                    + [self.update_subscription_settings(sub_id, client)]
                )
            )

    async def update_subscription_settings(self, subscription_id: str, client: MonitorManagementClient) -> None:
        return
        if (
            setting := await get_existing_diagnostic_setting(
                subscription_id, client.subscription_diagnostic_settings.list()
            )
        ) and setting.logs:
            # We have already added the setting for this subscription
            ...

    async def process_resource(
        self,
        client: MonitorManagementClient,
        sub_id: str,
        resource_id: str,
        assigned_config: DiagnosticSettingConfiguration,
    ) -> None:
        if current_config_id := self.diagnostic_settings_cache.get(sub_id, {}).get(resource_id):
            try:
                existing_setting = await get_existing_diagnostic_setting(
                    resource_id,
                    client.diagnostic_settings.list(resource_id),
                    existing_diagnostic_setting_name=get_diagnostic_setting_name(current_config_id),
                )
            except Exception:
                # TODO(AZINTS-2577) Error handling
                return

            if existing_setting and current_config_id == assigned_config.id:
                # The setting is already set and no changes are needed. All other cases should update the setting

                # do we ever want to update categories or anything?
                # or trust that the customer knows what they are doing if they modify the setting?
                return

        # covers 3 cases:
        # 1. no existing setting on this resource (and nothing in the DS cache)
        # 2. existing setting with different configuration
        # 3. settings was removed, we're re-adding it (with the new assignment)
        await self.set_diagnostic_setting(client, sub_id, resource_id, assigned_config)
        self.diagnostic_settings_cache.setdefault(sub_id, {})[resource_id] = assigned_config.id

    async def set_diagnostic_setting(
        self,
        client: MonitorManagementClient,
        sub_id: str,
        resource_id: str,
        config: DiagnosticSettingConfiguration,
    ) -> None:
        try:
            categories: list[str] = [
                category.name  # type: ignore
                async for category in client.diagnostic_settings_category.list(resource_id)
                if category.category_type == CategoryType.LOGS
            ]
            if not categories:
                log.debug("No log categories found for resource %s", resource_id)
                return

            await client.diagnostic_settings.create_or_update(
                resource_id,
                get_diagnostic_setting_name(config.id),
                get_diagnostic_setting(sub_id, self.resource_group, config, categories),
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
                    config,
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
    resource_group = get_config_option("RESOURCE_GROUP")
    assignment_cache, diagnostic_settings = await gather(
        read_cache(ASSIGNMENT_CACHE_BLOB), read_cache(DIAGNOSTIC_SETTINGS_CACHE_BLOB)
    )
    async with DiagnosticSettingsTask(assignment_cache, diagnostic_settings, resource_group) as task:
        await task.run()
    log.info("Task finished at %s", now())


if __name__ == "__main__":  # pragma: no cover
    run(main())
