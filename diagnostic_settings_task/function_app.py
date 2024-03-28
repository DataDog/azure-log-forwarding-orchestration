# stdlib
from asyncio import gather
from copy import deepcopy
from datetime import datetime
from json import dumps
from logging import ERROR, INFO, getLogger
from typing import AsyncIterable, Collection, Final, TypeVar
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

# project
if "FUNCTIONS_WORKER_RUNTIME" not in environ:
    # import as a module
    import diagnostic_settings_task.cache as cache
else:
    # import in script mode (azure function runtime)
    import cache  # type: ignore


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


class DiagnosticSettingsTask:
    def __init__(
        self,
        credential: DefaultAzureCredential,
        resource_cache_state: str,
        diagnostic_settings_cache_state: str,
        diagnostic_settings_cache: Out[str],
    ) -> None:
        self._cache = diagnostic_settings_cache
        self.resource_cache = cache.deserialize_resource_cache(resource_cache_state)
        self._diagnostic_settings_cache_initial = cache.deserialize_diagnostic_settings_cache(
            diagnostic_settings_cache_state
        )
        self.diagnostic_settings_cache = deepcopy(self._diagnostic_settings_cache_initial)
        self.credential = credential
        if (event_hub_name := environ.get(EVENT_HUB_NAME_SETTING)) and (
            event_hub_namespace := environ.get(EVENT_HUB_NAMESPACE_SETTING)
        ):
            self.EVENT_HUB_NAME: Final[str] = event_hub_name
            self.EVENT_HUB_NAMESPACE: Final[str] = event_hub_namespace
        else:
            raise ValueError("No event hub name/namespace found in environment")

    async def run(self) -> None:
        log.info(f"Crawling {len(self.resource_cache)} subscriptions")
        try:
            await gather(
                *[self.process_subscription(sub_id, resources) for sub_id, resources in self.resource_cache.items()]
            )
        finally:
            self.update_diagnostic_settings_cache()

    async def process_subscription(self, sub_id: str, resource_ids: Collection[str]) -> None:
        log.info(f"Crawling {len(resource_ids)} resources for subscription {sub_id}")
        async with MonitorManagementClient(self.credential, sub_id) as client:
            # client.management_group_diagnostic_settings.list("management_group_id") TODO: do we want to do anything with this?
            await gather(
                *(
                    [self.process_resource(client, sub_id, resource_id) for resource_id in resource_ids]
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
        sub_id: str,
        resource_id: str,
    ) -> None:
        if configuration := self.diagnostic_settings_cache.get(sub_id, {}).get(resource_id):
            setting_id = configuration["id"]
            try:
                existing_setting = await get_existing_diagnostic_setting(
                    resource_id,
                    client.diagnostic_settings.list(resource_id),
                    existing_diagnostic_setting_name=DIAGNOSTIC_SETTING_PREFIX + setting_id,
                )
            except Exception:
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
                    setting_id,
                    configuration["event_hub_name"],
                    configuration["event_hub_namespace"],
                )
        else:
            # We don't have a full configuration for this resource, we should add it
            await self.add_diagnostic_setting(
                client, sub_id, resource_id, str(uuid4()), self.EVENT_HUB_NAME, self.EVENT_HUB_NAMESPACE
            )

    async def add_diagnostic_setting(
        self,
        client: MonitorManagementClient,
        sub_id: str,
        resource_id: str,
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
            self.diagnostic_settings_cache.setdefault(sub_id, {})[resource_id] = {
                "id": diagnostic_setting_id,
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
                log.error(
                    "Resource %s already has a diagnostic setting with the same eventhub (namespace: %s name: %s)",
                    resource_id,
                    event_hub_namespace,
                    event_hub_name,
                )
                return

            log.error("Failed to add diagnostic setting for resource %s -- %s", resource_id, e.error)
        except Exception:
            log.error(
                "Unexpected error when trying to add diagnostic setting for resource %s", resource_id, exc_info=True
            )

    def update_diagnostic_settings_cache(self) -> None:
        if self.diagnostic_settings_cache != self._diagnostic_settings_cache_initial:
            self._cache.set(dumps(self.diagnostic_settings_cache))
            num_resources = sum(len(resources) for resources in self.diagnostic_settings_cache.values())
            log.info(f"Updated setting, {num_resources} resources stored in the settings cache")
        else:
            log.info("Diagnostic settings have not changed, no update needed")


def now() -> str:
    return datetime.now().isoformat()


app = FunctionApp()


@app.function_name(name=DIAGNOSTIC_SETTINGS_TASK_NAME)
@app.schedule(schedule="0 */6 * * * *", arg_name="req", run_on_startup=False)
@app.blob_input(
    arg_name="resourceCacheState",
    path=BLOB_STORAGE_CACHE + "/resources.json",
    connection=BLOB_CONNECTION_SETTING,
)
@app.blob_input(
    arg_name="diagnosticSettingsCacheState",
    path=BLOB_STORAGE_CACHE + "/settings.json",
    connection=BLOB_CONNECTION_SETTING,
)
@app.blob_output(
    arg_name="diagnosticSettingsCache",
    path=BLOB_STORAGE_CACHE + "/settings.json",
    connection=BLOB_CONNECTION_SETTING,
)
async def run_job(
    req: TimerRequest, resourceCacheState: str, diagnosticSettingsCacheState: str, diagnosticSettingsCache: Out[str]
) -> None:
    if req.past_due:
        log.info("The timer is past due!")
    log.info("Started task at %s", now())
    async with DefaultAzureCredential() as cred:
        await DiagnosticSettingsTask(
            cred, resourceCacheState, diagnosticSettingsCacheState, diagnosticSettingsCache
        ).run()
    log.info("Task finished at %s", now())
