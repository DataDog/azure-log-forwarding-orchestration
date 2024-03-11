# stdlib
from asyncio import gather
from copy import deepcopy
from datetime import datetime, timezone
from json import dumps, loads
from logging import ERROR, INFO, getLogger
from typing import AsyncIterable, NotRequired, TypeAlias, TypeVar, TypedDict
from uuid import uuid4
from os import environ

# 3p
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.functions import FunctionApp, TimerRequest, Out
from azure.mgmt.monitor.v2021_05_01_preview.aio import MonitorManagementClient
from azure.mgmt.monitor.v2021_05_01_preview.models import (
    DiagnosticSettingsResource,
    LogSettings,
    CategoryType,
    Resource,
)
from azure.identity.aio import DefaultAzureCredential

# silence azure logging except for errors
getLogger("azure").setLevel(ERROR)


DIAGNOSTIC_SETTINGS_TASK_NAME = "diagnostic_settings_task"
BLOB_STORAGE_CACHE = "resources-cache"
BLOB_CONNECTION_SETTING = "BLOB_CONNECTION_STRING"
EVENT_HUB_NAME_SETTING = "EVENT_HUB_NAME"
EVENT_HUB_NAMESPACE_SETTING = "EVENT_HUB_NAMESPACE"
DIAGNOSTIC_SETTING_PREFIX = "datadog_log_forwarding_"

log = getLogger(DIAGNOSTIC_SETTINGS_TASK_NAME)
log.setLevel(INFO)


class ResourceConfiguration(TypedDict):
    diagnostic_setting_id: NotRequired[str]
    event_hub_name: NotRequired[str]
    event_hub_namespace: NotRequired[str]


SubscriptionId: TypeAlias = str
ResourceId: TypeAlias = str
ResourceCache: TypeAlias = dict[SubscriptionId, dict[ResourceId, ResourceConfiguration]]

DiagnosticSettingType = TypeVar("DiagnosticSettingType", bound=Resource)


async def get_existing_diagnostic_setting(
    resource_id: ResourceId,
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
    except ResourceNotFoundError as e:
        log.warning("Resource %s not found: %s", resource_id, e.error)
        return
    except HttpResponseError as e:
        if e.error and e.error.code == "ResourceTypeNotSupported":
            # This resource does not support diagnostic settings
            return
        log.error("Failed to get diagnostic settings for %s", resource_id, exc_info=True)
        raise


class DiagnosticSettingsTask:
    def __init__(self, credential: DefaultAzureCredential, resources: str, cache: Out[str]) -> None:
        self._cache = cache
        self._resource_cache_initial_state: ResourceCache = loads(resources) if resources else {}
        """Cache of subscription_id to resource_id to diagnostic_setting_id, or None if the resource is new/has no diagnotic setting."""
        if not isinstance(self._resource_cache_initial_state, dict):
            raise ValueError(f"Cache is in an invalid format: {str(self._resource_cache_initial_state)[:100]}...")
        self.resource_cache: ResourceCache = deepcopy(self._resource_cache_initial_state)
        self.credential = credential

    async def run(self) -> None:
        log.info(f"Crawling {len(self.resource_cache)} subscriptions")
        try:
            await gather(
                *[
                    self.process_subscription(sub_id, resource_configurations)
                    for sub_id, resource_configurations in self.resource_cache.items()
                ]
            )
        finally:
            self.update_diagnostic_settings_cache()

    async def process_subscription(
        self, sub_id: SubscriptionId, resource_configurations: dict[ResourceId, ResourceConfiguration]
    ) -> None:
        log.info(f"Crawling {len(resource_configurations)} resources for subscription {sub_id}")
        async with MonitorManagementClient(self.credential, sub_id) as client:
            # client.management_group_diagnostic_settings.list("management_group_id") TODO: do we want to do anything with this?
            await gather(
                *(
                    [
                        self.process_resource(client, sub_id, resource_id, resource_configuration)
                        for resource_id, resource_configuration in resource_configurations.items()
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

    async def process_resource(
        self,
        client: MonitorManagementClient,
        sub_id: SubscriptionId,
        resource_id: ResourceId,
        resource_configuration: ResourceConfiguration,
    ) -> None:
        diagnostic_setting_id = resource_configuration.get("diagnostic_setting_id")
        event_hub_name = resource_configuration.get("event_hub_name")
        event_hub_namespace = resource_configuration.get("event_hub_namespace")

        if diagnostic_setting_id and event_hub_name and event_hub_namespace:
            existing_setting = await get_existing_diagnostic_setting(
                resource_id,
                client.diagnostic_settings.list(resource_id),
                existing_diagnostic_setting_name=DIAGNOSTIC_SETTING_PREFIX + diagnostic_setting_id,
            )

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
                    client, sub_id, resource_id, diagnostic_setting_id, event_hub_name, event_hub_namespace
                )
        else:
            # We don't have a full configuration for this resource, we should add it
            if (event_hub_name := environ.get(EVENT_HUB_NAME_SETTING)) and (
                event_hub_namespace := environ.get(EVENT_HUB_NAMESPACE_SETTING)
            ):
                await self.add_diagnostic_setting(
                    client, sub_id, resource_id, str(uuid4()), event_hub_name, event_hub_namespace
                )
            else:
                log.error("No event hub name/namespace found in environment")

    async def add_diagnostic_setting(
        self,
        client: MonitorManagementClient,
        sub_id: SubscriptionId,
        resource_id: ResourceId,
        diagnostic_setting_id: str,
        event_hub_name: str,
        event_hub_namespace: str,
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
            resource_group = "lfo"  # todo: programatically get resource group of the eventhub
            authorization_rule_id = f"/subscriptions/{sub_id}/resourcegroups/{resource_group}/providers/Microsoft.EventHub/namespaces/{event_hub_namespace}/authorizationrules/RootManageSharedAccessKey"
            await client.diagnostic_settings.create_or_update(
                resource_id,
                DIAGNOSTIC_SETTING_PREFIX + diagnostic_setting_id,
                DiagnosticSettingsResource(
                    event_hub_authorization_rule_id=authorization_rule_id,
                    event_hub_name=event_hub_name,
                    logs=[LogSettings(category=category.name, enabled=True) for category in categories],
                ),
            )
            self.resource_cache[sub_id][resource_id] = {
                "diagnostic_setting_id": diagnostic_setting_id,
                "event_hub_name": event_hub_name,
                "event_hub_namespace": event_hub_namespace,
            }
            log.info("Added diagnostic setting for resource %s", resource_id)
        except HttpResponseError as e:
            if e.error and e.error.code == "ResourceTypeNotSupported":
                # This resource does not support diagnostic settings
                return
            if "Resources should be in the same region" in str(e):
                # todo this should not happen in the real implementation, for now ignore
                return
            if "reused in different settings on the same category for the same resource" in str(e):
                # the cache is likely out of sync, for now lets raise an error
                log.error("Diagnostic setting for resource %s is out of sync with the cache", resource_id)
                return

            log.error("Failed to add diagnostic setting for resource %s -- %s", resource_id, e.error)
        except Exception:
            log.error(
                "Unexpected error when trying to add diagnostic setting for resource %s", resource_id, exc_info=True
            )

    def update_diagnostic_settings_cache(self) -> None:
        if self.resource_cache != self._resource_cache_initial_state:
            self._cache.set(dumps(self.resource_cache))
            num_resources = sum(len(resources) for resources in self.resource_cache.values())
            log.info(f"Updated Resources, {num_resources} resources stored in the cache")
        else:
            log.info("Resources have not changed, no update needed")


def now() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()


app = FunctionApp()


@app.function_name(name=DIAGNOSTIC_SETTINGS_TASK_NAME)
@app.schedule(schedule="0 */6 * * * *", arg_name="req", run_on_startup=False)
@app.blob_input(
    arg_name="resources",
    path=BLOB_STORAGE_CACHE + "/resources.json",
    connection=BLOB_CONNECTION_SETTING,
)
@app.blob_output(
    arg_name="cache",
    path=BLOB_STORAGE_CACHE + "/resources.json",
    connection=BLOB_CONNECTION_SETTING,
)
async def run_job(req: TimerRequest, resources: str, cache: Out[str]) -> None:
    if req.past_due:
        log.info("The timer is past due!")
    log.info("Started crawl at %s", now())
    async with DefaultAzureCredential() as cred:
        await DiagnosticSettingsTask(cred, resources, cache).run()
    log.info("Crawl finished at %s", now())
