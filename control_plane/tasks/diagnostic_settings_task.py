# stdlib
from asyncio import gather, run
from json import dumps
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

# dd
from datadog_api_client.v1 import ApiClient, Configuration
from datadog_api_client.v1.api.events_api import EventsApi
from datadog_api_client.v1.models import EventCreateRequest

# project
from cache.assignment_cache import ASSIGNMENT_CACHE_BLOB, deserialize_assignment_cache
from cache.common import InvalidCacheError, LogForwarderType, read_cache, write_cache
from cache.diagnostic_settings_cache import (
    DIAGNOSTIC_SETTINGS_COUNT,
    EVENT_CACHE_BLOB,
    SENT_EVENT,
    ResourceDict,
    deserialize_event_cache,
)
from cache.env import CONTROL_PLANE_ID_SETTING, RESOURCE_GROUP_SETTING, get_config_option
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

MAX_DIAGNOSTIC_SETTINGS = 5
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


class DiagnosticSettingsTask(Task):
    def __init__(self, assignment_cache_state: str, event_cache_state: str) -> None:
        super().__init__()

        self.resource_group = get_config_option(RESOURCE_GROUP_SETTING)
        self.diagnostic_settings_name = (
            DIAGNOSTIC_SETTING_PREFIX + get_config_option(CONTROL_PLANE_ID_SETTING)
        ).lower()

        assignment_cache = deserialize_assignment_cache(assignment_cache_state)
        if assignment_cache is None:
            raise InvalidCacheError("Assignment Cache is in an invalid format, failing this task until it is valid")
        self.assignment_cache = assignment_cache

        event_cache = deserialize_event_cache(event_cache_state)
        self.initial_event_cache = event_cache
        if event_cache is None:
            log.warning("Detected invalid event cache, cache will be reset")
            event_cache = {sub_id: {} for sub_id in self.assignment_cache}

        self.event_cache = event_cache

    async def run(self) -> None:
        log.info("Processing %s subscriptions", len(self.assignment_cache))
        await gather(*map(self.process_subscription, self.assignment_cache))

    async def process_subscription(self, sub_id: str) -> None:
        log.info("Processing subscription %s", sub_id)
        # we assume if a resource isn't in the assignment cache, then it has been deleted
        async with MonitorManagementClient(self.credential, sub_id) as client:
            # TODO: do we want to do anything with management group diagnostic settings?
            # client.management_group_diagnostic_settings.list("management_group_id")
            resources = [
                (resource, DiagnosticSettingConfiguration(config_id, region_config["configurations"][config_id]))
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

    def send_max_settings_reached_event(self, sub_id: str, resource_id: str) -> bool:
        config = Configuration()
        with ApiClient(config) as api_client:
            events_api = EventsApi(api_client)
            body = EventCreateRequest(
                title="Can't add diagnostic setting to resource - maximum number of diagnostic settings reached. This will prevent log forwarding for this resource.",
                text=f"Resource '{resource_id}' in subscription '{sub_id}' has reached the maximum number of diagnostic settings. Enabling log forwarding requires the addition of a DataDog diagnostic setting.",
                tags=["forwarder:lfo", "resource_id:" + resource_id, "subscription_id:" + sub_id],
                alert_type="warning",
            )
            try:
                events_api.create_event(body)  # type: ignore
                return True
            except Exception as e:
                log.error(f"Error while sending event to the Datadog: {e}")
                return False

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

        num_diag_settings = len(current_diagnostic_settings)
        new_setting_to_add = current_setting is None

        if (
            current_setting
            and current_setting.storage_account_id
            and current_setting.storage_account_id.lower()
            == get_storage_account_id(sub_id, self.resource_group, assigned_config.id)
        ):
            return  # current diagnostic setting is correctly configured

        self.update_event_cache(sub_id, resource_id, num_diag_settings)

        if num_diag_settings == MAX_DIAGNOSTIC_SETTINGS:
            if self.event_cache[sub_id][resource_id][SENT_EVENT] is False:
                event_sent_success = self.send_max_settings_reached_event(sub_id, resource_id)
                self.event_cache[sub_id][resource_id][SENT_EVENT] = event_sent_success
            log.warning(
                "Max number of diagnostic settings reached for resource %s in subscription %s, won't not add another",
                resource_id,
                sub_id,
            )
            return

        success = await self.create_or_update_diagnostic_setting(
            client,
            sub_id,
            resource_id,
            assigned_config,
            # keep the same categories selected (or add all if new), just fix the storage account
            categories=current_setting and [cast(str, log.category) for log in (current_setting.logs or [])],
        )

        if new_setting_to_add and success:
            self.event_cache[sub_id][resource_id][DIAGNOSTIC_SETTINGS_COUNT] = num_diag_settings + 1

    def update_event_cache(self, sub_id: str, resource_id: str, num_diag_settings: int) -> None:
        if self.event_cache.get(sub_id, None) is None:
            init_resource_dict: ResourceDict = {
                resource_id: {DIAGNOSTIC_SETTINGS_COUNT: num_diag_settings, SENT_EVENT: False}
            }
            self.event_cache[sub_id] = init_resource_dict
            return

        if self.event_cache[sub_id].get(resource_id, None) is None:
            self.event_cache[sub_id][resource_id] = {DIAGNOSTIC_SETTINGS_COUNT: num_diag_settings, SENT_EVENT: False}
            return

        if self.event_cache[sub_id] and self.event_cache[sub_id][resource_id]:
            self.event_cache[sub_id][resource_id][DIAGNOSTIC_SETTINGS_COUNT] = num_diag_settings

    async def create_or_update_diagnostic_setting(
        self,
        client: MonitorManagementClient,
        sub_id: str,
        resource_id: str,
        config: DiagnosticSettingConfiguration,
        categories: list[str] | None = None,
    ) -> bool:
        try:
            categories = categories or [
                cast(str, category.name)
                async for category in client.diagnostic_settings_category.list(resource_id)
                if category.category_type == CategoryType.LOGS
            ]
            if not categories:
                log.debug("No log categories found for resource %s", resource_id)
                return False

            await client.diagnostic_settings.create_or_update(
                resource_id,
                self.diagnostic_settings_name,
                get_diagnostic_setting(sub_id, self.resource_group, config, categories),
            )
            log.info("Added diagnostic setting for resource %s", resource_id)
            return True
        except HttpResponseError as e:
            if e.error and e.error.code == "ResourceTypeNotSupported":
                # This resource does not support diagnostic settings
                return False
            if "Resources should be in the same region" in str(e):
                # todo this should not happen in the real implementation, for now ignore
                return False
            if "reused in different settings on the same category for the same resource" in str(e):
                log.error(
                    "Resource %s already has a diagnostic setting with the same configuration: %s", resource_id, config
                )
                return False

            log.error("Failed to add diagnostic setting for resource %s -- %s", resource_id, e.error)
            return False
        except Exception:
            log.error(
                "Unexpected error when trying to add diagnostic setting for resource %s", resource_id, exc_info=True
            )
            return False

    async def write_caches(self) -> None:
        if self.event_cache == self.initial_event_cache:
            log.info("No changes to event cache, skipping write")
            return
        await write_cache(EVENT_CACHE_BLOB, dumps(self.event_cache, default=list))


async def main() -> None:
    basicConfig(level=INFO)
    log.info("Started task at %s", now())
    assignment_cache = await read_cache(ASSIGNMENT_CACHE_BLOB)
    event_cache = await read_cache(EVENT_CACHE_BLOB)
    async with DiagnosticSettingsTask(assignment_cache, event_cache) as task:
        await task.run()
    log.info("Task finished at %s", now())


if __name__ == "__main__":  # pragma: no cover
    run(main())
