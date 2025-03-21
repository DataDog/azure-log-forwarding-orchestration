# stdlib
from asyncio import gather, run
from copy import deepcopy
from json import dumps
from random import shuffle
from typing import NamedTuple, cast

# 3p
from azure.core.exceptions import HttpResponseError
from azure.mgmt.core.tools import parse_resource_id
from azure.mgmt.monitor.v2021_05_01_preview.aio import MonitorManagementClient
from azure.mgmt.monitor.v2021_05_01_preview.models import (
    CategoryType,
    DiagnosticSettingsResource,
    LogSettings,
)

# dd
from datadog_api_client.v1.api.events_api import EventsApi
from datadog_api_client.v1.models import EventCreateRequest, EventCreateResponse

# project
from cache.assignment_cache import ASSIGNMENT_CACHE_BLOB, deserialize_assignment_cache
from cache.common import InvalidCacheError, LogForwarderType, write_cache
from cache.diagnostic_settings_cache import (
    EVENT_CACHE_BLOB,
    SENT_EVENT,
    EventDict,
    deserialize_event_cache,
    remove_cached_resource,
    update_cached_setting_count,
)
from cache.env import CONTROL_PLANE_ID_SETTING, RESOURCE_GROUP_SETTING, get_config_option
from cache.resources_cache import FILTERED_IN_KEY, RESOURCE_CACHE_BLOB, deserialize_resource_cache
from tasks.common import (
    get_event_hub_name,
    get_event_hub_namespace,
    get_resource_group_id,
    get_storage_account_id,
)
from tasks.concurrency import collect
from tasks.task import Task, task_main

DD_REQUEST_TIMEOUT = 5  # seconds
DIAGNOSTIC_SETTINGS_TASK_NAME = "diagnostic_settings_task"
MAX_DIAGNOSTIC_SETTINGS = 5
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
    NAME = DIAGNOSTIC_SETTINGS_TASK_NAME

    def __init__(self, resource_cache_state: str, assignment_cache_state: str, event_cache_state: str) -> None:
        super().__init__()

        self.resource_group = get_config_option(RESOURCE_GROUP_SETTING)
        self.diagnostic_settings_name = (
            DIAGNOSTIC_SETTING_PREFIX + get_config_option(CONTROL_PLANE_ID_SETTING)
        ).lower()

        resource_cache, _ = deserialize_resource_cache(resource_cache_state)
        if resource_cache is None:
            raise InvalidCacheError("Resource Cache is in an invalid format, failing this task until it is valid")
        self.resource_cache = resource_cache

        assignment_cache = deserialize_assignment_cache(assignment_cache_state)
        if assignment_cache is None:
            raise InvalidCacheError("Assignment Cache is in an invalid format, failing this task until it is valid")
        self.assignment_cache = assignment_cache

        event_cache = deserialize_event_cache(event_cache_state)
        self.initial_event_cache = event_cache
        if event_cache is None:
            self.log.warning("Detected invalid event cache, cache will be reset")
            event_cache = {sub_id: {} for sub_id in self.assignment_cache}

        self.event_cache = deepcopy(event_cache)
        self.events_api = EventsApi(self._datadog_client)

    async def run(self) -> None:
        self.log.info("Processing %s subscriptions", len(self.assignment_cache))
        await gather(*map(self.process_subscription, self.assignment_cache))

    async def process_subscription(self, sub_id: str) -> None:
        self.log.info("Processing subscription %s", sub_id)
        # we assume if a resource isn't in the assignment cache then is has been deleted
        async with MonitorManagementClient(self.credential, sub_id) as client:
            # TODO: do we want to do anything with management group diagnostic settings?
            # client.management_group_diagnostic_settings.list("management_group_id")
            resources = [
                (
                    resource,
                    region,
                    DiagnosticSettingConfiguration(config_id, region_config["configurations"][config_id]),
                )
                for region, region_config in self.assignment_cache[sub_id].items()
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
                        region,
                        ds,
                    )
                    for resource, region, ds in resources
                ),
            )

    async def update_subscription_settings(self, subscription_id: str, client: MonitorManagementClient) -> None:
        # TODO: do we want to do anything with management group diagnostic settings?
        # i.e. client.subscription_diagnostic_settings.list()
        return

    async def send_max_settings_reached_event(self, sub_id: str, resource_id: str) -> bool:
        parsed_resource = parse_resource_id(resource_id)
        parse_success = len(parsed_resource) > 1
        event_tags = ["forwarder:lfo", "subscription_id:" + sub_id]
        if parse_success:
            event_tags.extend(
                [
                    "resource_type:" + cast(str, parsed_resource["type"]),
                    "resource_provider:" + cast(str, parsed_resource["namespace"]),
                    "resource_group:" + cast(str, parsed_resource["resource_group"]),
                ]
            )
        else:
            self.log.error("Failed to parse resource id %s", resource_id)

        body = EventCreateRequest(
            title=f"Log forwarding disabled for Azure resource {cast(str, parsed_resource['name']) if parse_success else None}",
            text=f"Log forwarding cannot be enabled for resource '{resource_id}' because it already has the maximum number of diagnostic settings configured. The addition of a DataDog diagnostic setting is necessary for log forwarding.",
            tags=event_tags,
            alert_type="warning",
        )

        try:
            response: EventCreateResponse = await self.events_api.create_event(body)  # type: ignore
        except Exception as e:
            self.log.error(f"Error while sending event to Datadog: {e}")
            return False

        if errors := response.get("errors", []):
            self.log.error(f"Error(s) while sending event to Datadog: {errors}")
            return False
        if response.status != "ok":
            self.log.error(f"Http error while sending event to Datadog: {response}")
            return False

        self.log.info(
            "Sent max diagnostic setting event for resource %s",
            resource_id,
            extra={"subscription_id": sub_id},
        )
        return True

    async def process_resource(
        self,
        client: MonitorManagementClient,
        sub_id: str,
        resource_id: str,
        region: str,
        assigned_config: DiagnosticSettingConfiguration,
    ) -> None:
        try:
            current_diagnostic_settings = await collect(client.diagnostic_settings.list(resource_id))
        except HttpResponseError as e:
            if e.error and e.error.code and e.error.code.lower() == "resourcetypenotsupported":
                self.log.warning("Resource type for %s unsupported, skipping", resource_id)
                return
            self.log.exception("Failed to get diagnostic settings for resource %s", resource_id)
            return

        current_setting = next(
            filter(
                lambda ds: cast(str, ds.name).lower() == self.diagnostic_settings_name,
                current_diagnostic_settings,
            ),
            None,
        )

        num_diag_settings = len(current_diagnostic_settings)
        self.event_cache.setdefault(sub_id, {}).setdefault(
            resource_id, EventDict(diagnostic_settings_count=num_diag_settings, sent_event=False)
        )

        filtered_in = (
            (self.resource_cache or {}).get(sub_id, {}).get(region, {}).get(resource_id, {}).get(FILTERED_IN_KEY, True)
        )

        if (
            current_setting
            and current_setting.storage_account_id
            and current_setting.storage_account_id.lower()
            == get_storage_account_id(sub_id, self.resource_group, assigned_config.id)
        ):
            # diagnostic setting exists on resource and is configured correctly
            # check if we should delete the setting because the resource has recently been filtered out
            if not filtered_in:
                self.log.info(
                    "Resource %s has been filtered out and has diagnostic setting %s, deleting setting",
                    resource_id,
                    self.diagnostic_settings_name,
                )
                if await self.delete_diagnostic_setting(client, resource_id):
                    remove_cached_resource(self.event_cache, sub_id, resource_id)
            return

        if not filtered_in:
            # prevent adding a new diagnostic setting
            update_cached_setting_count(self.event_cache, sub_id, resource_id, num_diag_settings)
            return

        if num_diag_settings >= MAX_DIAGNOSTIC_SETTINGS:
            if self.event_cache[sub_id][resource_id][SENT_EVENT] is False:
                event_sent_success = await self.send_max_settings_reached_event(sub_id, resource_id)
                self.event_cache[sub_id][resource_id][SENT_EVENT] = event_sent_success

            self.log.warning(
                "Max number of diagnostic settings reached for resource %s, will not add another",
                resource_id,
                extra={"subscription_id": sub_id},
            )
            return

        add_setting_success = await self.create_or_update_diagnostic_setting(
            client,
            sub_id,
            resource_id,
            assigned_config,
            # keep the same categories selected (or add all if new), just fix the storage account
            categories=current_setting and [cast(str, log.category) for log in (current_setting.logs or [])],
        )

        if current_setting is None and add_setting_success:
            update_cached_setting_count(self.event_cache, sub_id, resource_id, num_diag_settings + 1)

    async def create_or_update_diagnostic_setting(
        self,
        client: MonitorManagementClient,
        sub_id: str,
        resource_id: str,
        config: DiagnosticSettingConfiguration,
        categories: list[str] | None = None,
    ) -> bool:
        """Creates or updates a diagnostic setting for an Azure resource

        Returns True if the diagnostic setting was successfully created or updated, False otherwise
        """
        try:
            categories = categories or [
                cast(str, category.name)
                async for category in client.diagnostic_settings_category.list(resource_id)
                if category.category_type == CategoryType.LOGS
            ]
            if not categories:
                self.log.debug("No log categories found for resource %s", resource_id)
                return False

            await client.diagnostic_settings.create_or_update(
                resource_id,
                self.diagnostic_settings_name,
                get_diagnostic_setting(sub_id, self.resource_group, config, categories),
            )
            self.log.info("Added diagnostic setting for resource %s", resource_id)
            return True
        except HttpResponseError as e:
            if e.error and e.error.code == "ResourceTypeNotSupported":
                # This resource does not support diagnostic settings
                return False
            if "Resources should be in the same region" in str(e):
                # todo this should not happen in the real implementation, for now ignore
                return False
            if "reused in different settings on the same category for the same resource" in str(e):
                self.log.error(
                    "Resource %s already has a diagnostic setting with the same configuration: %s", resource_id, config
                )
                return False

            self.log.error("Failed to add diagnostic setting for resource %s -- %s", resource_id, e.error)
            return False
        except Exception:
            self.log.exception("Unexpected error when trying to add diagnostic setting for resource %s", resource_id)
            return False

    async def delete_diagnostic_setting(self, client: MonitorManagementClient, resource_id: str) -> bool:
        """Deletes a diagnostic setting for an Azure resource

        Returns True if the diagnostic setting was successfully deleted, False otherwise
        """
        try:
            await client.diagnostic_settings.delete(resource_id, self.diagnostic_settings_name)
        except Exception:
            self.log.exception("Failed to delete diagnostic setting for resource %s", resource_id)

        return True

    async def write_caches(self) -> None:
        if self.event_cache == self.initial_event_cache:
            self.log.info("No changes to event cache, skipping write")
            return
        await write_cache(EVENT_CACHE_BLOB, dumps(self.event_cache))


async def main() -> None:
    await task_main(DiagnosticSettingsTask, [RESOURCE_CACHE_BLOB, ASSIGNMENT_CACHE_BLOB, EVENT_CACHE_BLOB])


if __name__ == "__main__":  # pragma: no cover
    run(main())
