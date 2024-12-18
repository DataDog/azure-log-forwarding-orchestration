# stdlib
from asyncio import gather, run
from logging import ERROR, INFO, basicConfig, getLogger
from random import shuffle
from typing import NamedTuple, cast

# 3p
from azure.core.exceptions import HttpResponseError
from azure.mgmt.monitor.v2021_05_01_preview.aio import MonitorManagementClient
from azure.mgmt.monitor.v2021_05_01_preview.models import (
    CategoryType,
    DiagnosticSettingsResource,
    LogSettings,
)

# project
from cache.assignment_cache import ASSIGNMENT_CACHE_BLOB, deserialize_assignment_cache
from cache.common import (
    InvalidCacheError,
    LogForwarderType,
    get_config_option,
    read_cache,
)
from tasks.common import (
    get_event_hub_name,
    get_event_hub_namespace,
    get_resource_group_id,
    get_storage_account_id,
    now,
)
from tasks.concurrency import collect
from tasks.task import Task

# silence azure logging except for errors
getLogger("azure").setLevel(ERROR)

DIAGNOSTIC_SETTINGS_TASK_NAME = "diagnostic_settings_task"
log = getLogger(DIAGNOSTIC_SETTINGS_TASK_NAME)


DIAGNOSTIC_SETTING_PREFIX = "datadog_log_forwarding_"


def get_authorization_rule_id(sub_id: str, resource_group: str, config_id: str) -> str:  # pragma: no cover
    # no test coverage because we don't have event hub support yet
    return (
        get_resource_group_id(sub_id, resource_group)
        + "/providers/microsoft.eventhub/namespaces/"
        + get_event_hub_namespace(config_id)
        + "/authorizationrules/rootmanagesharedaccesskey"
    )


class DiagnosticSettingConfiguration(NamedTuple):
    "Convenience Tuple for holding the configuration of a diagnostic setting"

    id: str
    type: LogForwarderType


def get_diagnostic_setting(
    sub_id: str,
    resource_group: str,
    config: DiagnosticSettingConfiguration,
    categories: list[str],
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


class DiagnosticSettingsTask(Task):
    def __init__(self, assignment_cache_state: str) -> None:
        super().__init__()

        self.resource_group = get_config_option("RESOURCE_GROUP")
        self.diagnostic_settings_name = (DIAGNOSTIC_SETTING_PREFIX + get_config_option("CONTROL_PLANE_ID")).lower()

        # read caches
        assignment_cache = deserialize_assignment_cache(assignment_cache_state)
        if assignment_cache is None:
            raise InvalidCacheError("Assignment Cache is in an invalid format, failing this task until it is valid")
        self.assignment_cache = assignment_cache

    async def run(self) -> None:
        log.info("Processing %s subscriptions", len(self.assignment_cache))
        await gather(*map(self.process_subscription, self.assignment_cache))

    async def process_subscription(self, sub_id: str) -> None:
        log.info("Processing subscription %s", sub_id)
        # we assume if a resource isn't in the assignment cache then is has been deleted
        async with MonitorManagementClient(self.credential, sub_id) as client:
            # TODO: do we want to do anything with management group diagnostic settings?
            # client.management_group_diagnostic_settings.list("management_group_id")
            resources = [
                (
                    resource,
                    DiagnosticSettingConfiguration(config_id, region_config["configurations"][config_id]),
                )
                for region_config in self.assignment_cache[sub_id].values()
                for resource, config_id in region_config["resources"].items()
            ]
            shuffle(resources)
            await gather(
                self.update_subscription_settings(sub_id, client),
                *(
                    self.process_resource(
                        client,
                        sub_id,
                        resource,
                        ds,
                    )
                    for resource, ds in resources
                ),
            )

    async def update_subscription_settings(self, subscription_id: str, client: MonitorManagementClient) -> None:
        # TODO: do we want to do anything with management group diagnostic settings?
        # i.e. client.subscription_diagnostic_settings.list()
        return

    async def process_resource(
        self,
        client: MonitorManagementClient,
        sub_id: str,
        resource_id: str,
        assigned_config: DiagnosticSettingConfiguration,
    ) -> None:
        try:
            current_diagnostic_settings = await collect(client.diagnostic_settings.list(resource_id))
        except HttpResponseError as e:
            if e.error and e.error.code and e.error.code.lower() == "resourcetypenotsupported":
                log.warning("Resource type for %s unsupported, skipping", resource_id)
                return
            log.exception("Failed to get diagnostic settings for resource %s", resource_id)
            return

        current_setting = next(
            filter(
                lambda ds: cast(str, ds.name).lower() == self.diagnostic_settings_name,
                current_diagnostic_settings,
            ),
            None,
        )

        if (
            current_setting
            and current_setting.storage_account_id
            and current_setting.storage_account_id.lower()
            == get_storage_account_id(sub_id, self.resource_group, assigned_config.id)
        ):
            return  # it is set up properly already

        # otherwise fix it
        await self.set_diagnostic_setting(
            client,
            sub_id,
            resource_id,
            assigned_config,
            # keep the same categories selected (or add all if new), just fix the storage account
            categories=current_setting and [cast(str, log.category) for log in (current_setting.logs or [])],
        )

    async def set_diagnostic_setting(
        self,
        client: MonitorManagementClient,
        sub_id: str,
        resource_id: str,
        config: DiagnosticSettingConfiguration,
        categories: list[str] | None = None,
    ) -> None:
        try:
            categories = categories or [
                cast(str, category.name)
                async for category in client.diagnostic_settings_category.list(resource_id)
                if category.category_type == CategoryType.LOGS
            ]
            if not categories:
                log.debug("No log categories found for resource %s", resource_id)
                return

            await client.diagnostic_settings.create_or_update(
                resource_id,
                self.diagnostic_settings_name,
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

            log.error(
                "Failed to add diagnostic setting for resource %s -- %s",
                resource_id,
                e.error,
            )
        except Exception:
            log.error(
                "Unexpected error when trying to add diagnostic setting for resource %s",
                resource_id,
                exc_info=True,
            )

    async def write_caches(self) -> None:
        pass  # nothing to do here


async def main() -> None:
    basicConfig(level=INFO)
    log.info("Started task at %s", now())
    assignment_cache = await read_cache(ASSIGNMENT_CACHE_BLOB)
    async with DiagnosticSettingsTask(assignment_cache) as task:
        await task.run()
    log.info("Task finished at %s", now())


if __name__ == "__main__":  # pragma: no cover
    run(main())
