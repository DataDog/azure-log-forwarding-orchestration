# stdlib
from asyncio import Lock, gather, run
from collections.abc import Awaitable, Coroutine
from copy import deepcopy
from json import dumps
from logging import DEBUG, INFO, basicConfig, getLogger
from types import TracebackType
from typing import Any, AsyncContextManager, Self, TypeVar
from uuid import uuid4

# 3p
from aiohttp import ClientSession
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.storage.v2023_05_01.aio import StorageManagementClient
from azure.mgmt.storage.v2023_05_01.models import (
    PublicNetworkAccess,
    Sku,
    StorageAccountCreateParameters,
    StorageAccountKey,
)
from azure.mgmt.web.v2023_12_01.aio import WebSiteManagementClient
from azure.mgmt.web.v2023_12_01.models import (
    AppServicePlan,
    ManagedServiceIdentity,
    NameValuePair,
    Site,
    SiteConfig,
    SkuDescription,
)
from azure.storage.blob.aio import ContainerClient
from tenacity import RetryCallState, retry, stop_after_attempt

# project
from cache.assignment_cache import ASSIGNMENT_CACHE_BLOB, deserialize_assignment_cache
from cache.common import (
    STORAGE_ACCOUNT_TYPE,
    DiagnosticSettingType,
    InvalidCacheError,
    get_app_service_plan_name,
    get_config_option,
    get_function_app_name,
    get_storage_account_name,
    read_cache,
    write_cache,
)
from cache.resources_cache import RESOURCE_CACHE_BLOB, deserialize_resource_cache
from tasks.task import Task, now, wait_for_resource

SCALING_TASK_NAME = "scaling_task"

BLOB_FORWARDER_DATA_CONTAINER, BLOB_FORWARDER_DATA_BLOB = "blob-forwarder", "data.zip"


log = getLogger(SCALING_TASK_NAME)
log.setLevel(DEBUG)


async def is_exception_retryable(state: RetryCallState) -> bool:
    if (future := state.outcome) and (e := future.exception()):
        if isinstance(e, HttpResponseError):
            return e.status_code is not None and (e.status_code == 429 or e.status_code >= 500)
    return False


T = TypeVar("T")


async def ignore_exception_type(exc: type[BaseException], a: Awaitable[T]) -> T | None:
    try:
        return await a
    except exc:
        return None


class LogForwarderClient(AsyncContextManager):
    def __init__(self, credential: DefaultAzureCredential, subscription_id: str, resource_group: str) -> None:
        self.control_plane_storage_connection_string = get_config_option("AzureWebJobsStorage")
        self._credential = credential
        self.web_client = WebSiteManagementClient(credential, subscription_id)
        self.storage_client = StorageManagementClient(credential, subscription_id)
        self.rest_client = ClientSession()
        self.resource_group = resource_group
        self._blob_forwarder_data_lock = Lock()
        self._blob_forwarder_data: bytes | None = None

    async def __aenter__(self) -> Self:
        await gather(self.web_client.__aenter__(), self.storage_client.__aenter__(), self.rest_client.__aenter__())
        token = await self._credential.get_token("https://management.azure.com/.default")
        self.rest_client.headers["Authorization"] = f"Bearer {token.token}"
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        await gather(
            self.web_client.__aexit__(exc_type, exc_val, exc_tb),
            self.storage_client.__aexit__(exc_type, exc_val, exc_tb),
            self.rest_client.__aexit__(exc_type, exc_val, exc_tb),
        )

    async def create_log_forwarder(self, region: str, config_id: str) -> DiagnosticSettingType:
        storage_account_name = get_storage_account_name(config_id)
        app_service_plan_name = get_app_service_plan_name(config_id)
        log.info(
            "Creating log forwarder storage (%s) and app service plan (%s)",
            storage_account_name,
            app_service_plan_name,
        )
        # storage account
        storage_account_future = await self.storage_client.storage_accounts.begin_create(
            resource_group_name=self.resource_group,
            account_name=storage_account_name,
            parameters=StorageAccountCreateParameters(
                sku=Sku(
                    # TODO (AZINTS-2646): figure out which SKU we should be using here
                    name="Standard_LRS"
                ),
                kind="StorageV2",
                location=region,
                public_network_access=PublicNetworkAccess.DISABLED,
            ),
        )
        # app service plan
        app_service_plan_future = await self.web_client.app_service_plans.begin_create_or_update(
            self.resource_group,
            app_service_plan_name,
            AppServicePlan(
                location=region,
                kind="linux",
                reserved=True,
                sku=SkuDescription(
                    # TODO: figure out which SKU we should be using here
                    tier="Basic",
                    name="B1",
                ),
            ),
        )
        try:
            storage_account, app_service_plan = await gather(
                wait_for_resource(
                    storage_account_future,
                    lambda: self.storage_client.storage_accounts.get_properties(
                        self.resource_group, storage_account_name
                    ),
                ),
                wait_for_resource(
                    app_service_plan_future,
                    lambda: self.web_client.app_service_plans.get(self.resource_group, app_service_plan_name),
                ),
            )
            log.info(
                "Created log forwarder storage account (%s) and app service plan (%s)",
                storage_account.id,
                app_service_plan.id,
            )
        except Exception:
            log.exception("Failed to create storage account and/or app service plan")
            raise

        function_app_name = get_function_app_name(config_id)
        log.info("Creating log forwarder app: %s", function_app_name)
        connection_string = await self.get_connection_string(storage_account_name)
        function_app_poller = await self.web_client.web_apps.begin_create_or_update(
            self.resource_group,
            function_app_name,
            Site(
                location=region,
                kind="functionapp",
                identity=ManagedServiceIdentity(type="SystemAssigned"),
                server_farm_id=app_service_plan.id,
                https_only=True,
                site_config=SiteConfig(
                    app_settings=[
                        NameValuePair(name="FUNCTIONS_WORKER_RUNTIME", value="custom"),
                        NameValuePair(name="AzureWebJobsStorage", value=connection_string),
                        NameValuePair(name="FUNCTIONS_EXTENSION_VERSION", value="~4"),
                    ]
                ),
            ),
        )
        try:
            function_app, blob_forwarder_data = await gather(
                wait_for_resource(
                    function_app_poller, lambda: self.web_client.web_apps.get(self.resource_group, function_app_name)
                ),
                self.get_blob_forwarder_data(),
            )
        except Exception:
            log.exception("Failed to create function app and/or get blob forwarder data")
            raise
        log.info("Created log forwarder function app: %s", function_app.id)

        # deploy code to function app
        log.info("Deploying log forwarder code to function app: %s", function_app.id)
        resp = await self.rest_client.post(
            f"https://{function_app_name}.scm.azurewebsites.net/api/publish?type=zip",
            data=blob_forwarder_data,
        )
        resp.raise_for_status()
        body = await resp.text()
        log.info(
            "Deployed log forwarder code to function app (zip size %s)\nstatus code: %s\nbody: %s",
            len(blob_forwarder_data),
            resp.status,
            body,
        )

        # for now this is the only type we support
        return STORAGE_ACCOUNT_TYPE

    async def get_connection_string(self, storage_account_name: str) -> str:
        keys_result = await self.storage_client.storage_accounts.list_keys(self.resource_group, storage_account_name)
        keys: list[StorageAccountKey] = keys_result.keys  # type: ignore
        if len(keys) == 0:
            raise ValueError("No keys found for storage account")
        key = keys[0].value
        return f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={key};EndpointSuffix=core.windows.net"

    async def get_blob_forwarder_data(self) -> bytes:
        async with self._blob_forwarder_data_lock:
            if self._blob_forwarder_data is None:
                async with ContainerClient.from_connection_string(
                    self.control_plane_storage_connection_string, BLOB_FORWARDER_DATA_CONTAINER
                ) as client:
                    stream = await client.download_blob(BLOB_FORWARDER_DATA_BLOB)
                    self._blob_forwarder_data = await stream.content_as_bytes(max_concurrency=4)
            return self._blob_forwarder_data

    async def delete_log_forwarder(self, forwarder_id: str, *, raise_error: bool = True, max_attempts: int = 3) -> bool:
        """Deletes the Log forwarder, returns True if successful, False otherwise"""

        @retry(stop=stop_after_attempt(max_attempts), retry=is_exception_retryable)
        async def _delete_forwarder():
            log.info("Attempting to delete log forwarder %s", forwarder_id)
            await gather(
                ignore_exception_type(
                    ResourceNotFoundError,
                    self.web_client.web_apps.delete(
                        self.resource_group, get_function_app_name(forwarder_id), delete_empty_server_farm=True
                    ),
                ),
                ignore_exception_type(
                    ResourceNotFoundError,
                    self.storage_client.storage_accounts.delete(
                        self.resource_group, get_storage_account_name(forwarder_id)
                    ),
                ),
            )
            log.info("Deleted log forwarder %s", forwarder_id)

        try:
            await _delete_forwarder()
            return True
        except Exception:
            if raise_error:
                raise
            return False


class ScalingTask(Task):
    def __init__(self, resource_cache_state: str, assignment_cache_state: str, resource_group: str) -> None:
        super().__init__()
        self.resource_group = resource_group

        # Resource Cache
        success, resource_cache = deserialize_resource_cache(resource_cache_state)
        if not success:
            raise InvalidCacheError("Resource Cache is in an invalid format, failing this task until it is valid")
        self.resource_cache = resource_cache

        # Assignment Cache
        success, assignment_cache = deserialize_assignment_cache(assignment_cache_state)
        if not success:
            log.warning("Assignment Cache is in an invalid format, task will reset the cache")
            assignment_cache = {}
        self._assignment_cache_initial_state = assignment_cache
        self.assignment_cache = deepcopy(assignment_cache)

    async def run(self) -> None:
        log.info("Running for %s subscriptions: %s", len(self.resource_cache), list(self.resource_cache.keys()))
        all_subscriptions = set(self.resource_cache.keys()) | set(self.assignment_cache.keys())
        await gather(*(self.process_subscription(sub_id) for sub_id in all_subscriptions))

    async def process_subscription(self, subscription_id: str) -> None:
        previous_region_assignments = set(self._assignment_cache_initial_state.get(subscription_id, {}).keys())
        current_regions = set(self.resource_cache.get(subscription_id, {}).keys())
        regions_to_add = current_regions - previous_region_assignments
        regions_to_remove = previous_region_assignments - current_regions
        async with LogForwarderClient(self.credential, subscription_id, self.resource_group) as client:
            tasks: list[Coroutine[Any, Any, None]] = []
            if regions_to_add:
                tasks.extend(self.create_log_forwarder(client, subscription_id, region) for region in regions_to_add)
            if regions_to_remove:
                tasks.extend(
                    self.delete_region_log_forwarders(client, subscription_id, region) for region in regions_to_remove
                )
            await gather(*tasks)

        self.update_assignments(subscription_id)

    async def create_log_forwarder(
        self,
        client: LogForwarderClient,
        subscription_id: str,
        region: str,
    ) -> None:
        log.info("Creating log forwarder for subscription %s in region %s", subscription_id, region)
        config_id = str(uuid4())[-12:]  # take the last section since we are length limited
        try:
            config_type = await client.create_log_forwarder(region, config_id)
        except Exception:
            log.exception("Failed to create log forwarder %s, cleaning up", config_id)
            success = await client.delete_log_forwarder(config_id, raise_error=False)
            if not success:
                log.error("Failed to clean up log forwarder %s, manual intervention required", config_id)
            return
        self.assignment_cache.setdefault(subscription_id, {})[region] = {
            "configurations": {config_id: config_type},
            "resources": {},
        }

    async def delete_region_log_forwarders(
        self,
        client: LogForwarderClient,
        subscription_id: str,
        region: str,
    ) -> None:
        log.info("Deleting log forwarder for subscription %s in region %s", subscription_id, region)
        await gather(
            *(
                client.delete_log_forwarder(forwarder_id)
                for forwarder_id in self.assignment_cache[subscription_id][region]["configurations"]
            )
        )
        del self.assignment_cache[subscription_id][region]

    def update_assignments(self, sub_id: str) -> None:
        for region, region_config in self.assignment_cache[sub_id].items():
            diagnostic_setting_configurations = region_config["configurations"]
            assert (
                len(diagnostic_setting_configurations) == 1
            )  # TODO(AZINTS-2388) right now we only have one config per region

            # just use the one config we have for now
            config_id = list(diagnostic_setting_configurations.keys())[0]

            resource_assignments = region_config["resources"]

            # update all the resource assignments to use the new config
            for resource_id in self.resource_cache[sub_id][region]:
                resource_assignments[resource_id] = config_id

    async def write_caches(self) -> None:
        if self.assignment_cache == self._assignment_cache_initial_state:
            log.info("Assignments have not changed, no update needed")
            return
        await write_cache(ASSIGNMENT_CACHE_BLOB, dumps(self.assignment_cache))
        log.info("Updated assignments stored in the cache")


async def main() -> None:
    basicConfig(level=INFO)
    log.info("Started task at %s", now())
    resource_group = get_config_option("RESOURCE_GROUP")
    resources_cache_state, assignment_cache_state = await gather(
        read_cache(RESOURCE_CACHE_BLOB),
        read_cache(ASSIGNMENT_CACHE_BLOB),
    )
    async with ScalingTask(resources_cache_state, assignment_cache_state, resource_group) as task:
        await task.run()
    log.info("Task finished at %s", now())


if __name__ == "__main__":
    run(main())
