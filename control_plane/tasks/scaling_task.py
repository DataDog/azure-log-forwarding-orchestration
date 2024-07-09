# stdlib
from asyncio import Lock, gather, run
from copy import deepcopy
from json import dumps
from logging import DEBUG, getLogger
from types import TracebackType
from typing import Any, Coroutine, AsyncContextManager, Self, cast
from uuid import UUID

# 3p
from aiohttp import ClientSession
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.web.v2023_12_01.aio import WebSiteManagementClient
from azure.mgmt.web.v2023_12_01.models import AppServicePlan, SkuDescription, Site, SiteConfig, NameValuePair
from azure.mgmt.storage.v2023_05_01.aio import StorageManagementClient
from azure.mgmt.storage.v2023_05_01.models import StorageAccountCreateParameters, Sku, StorageAccountKey
from azure.storage.blob.aio import ContainerClient

# project
from cache.assignment_cache import ASSIGNMENT_CACHE_BLOB, deserialize_assignment_cache
from cache.common import DiagnosticSettingConfiguration, InvalidCacheError, read_cache, write_cache
from cache.resources_cache import RESOURCE_CACHE_BLOB, deserialize_resource_cache
from tasks.task import Task, get_config_option, now


SCALING_TASK_NAME = "scaling_task"
BLOB_FORWARDER_DATA_CONTAINER, BLOB_FORWARDER_DATA_BLOB = "blob-forwarder", "data.zip"


log = getLogger(SCALING_TASK_NAME)
log.setLevel(DEBUG)


class LogForwarderClient(AsyncContextManager):
    def __init__(self, credential: DefaultAzureCredential, subscription_id: str, resource_group: str) -> None:
        self.control_plane_storage_connection_string = get_config_option("AzureWebJobsStorage")
        self._credential = credential
        self.web_client = WebSiteManagementClient(credential, subscription_id)
        self.storage_client = StorageManagementClient(credential, subscription_id)
        self.rest_client = ClientSession()
        self.subscription_id = subscription_id
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

    async def create_log_forwarder(self, region: str) -> DiagnosticSettingConfiguration:
        log_forwarder_id = str(UUID())
        # storage account
        storage_account_name = f"log-forwarder-storage-{log_forwarder_id}"
        storage_account_future = await self.storage_client.storage_accounts.begin_create(
            resource_group_name=self.resource_group,
            account_name=storage_account_name,
            parameters=StorageAccountCreateParameters(
                sku=Sku(
                    # TODO: figure out which SKU we should be using here
                    name="Standard_LRS"
                ),
                kind="StorageV2",
                location=region,
            ),
        )
        # app service plan
        app_service_plan_name = f"log-forwarder-plan-{log_forwarder_id}"
        app_service_plan_future = await self.web_client.app_service_plans.begin_create_or_update(
            self.resource_group,
            app_service_plan_name,
            AppServicePlan(
                location=region,
                sku=SkuDescription(
                    # TODO: figure out which SKU we should be using here
                    name="Y1",
                    tier="Dynamic",
                    size="Y1",
                    family="Y",
                    capacity=0,
                ),
            ),
        )
        try:
            storage_account, app_service_plan = await gather(
                storage_account_future.result(), app_service_plan_future.result()
            )
        except Exception:
            log.exception("Failed to create log forwarder resources")
            raise

        # self.storage_client.blob_containers.create()
        function_app_name = f"blob-log-forwarder-{log_forwarder_id}"
        connection_string = await self.get_connection_string(storage_account_name)
        function_app_future = await self.web_client.web_apps.begin_create_or_update(
            self.resource_group,
            function_app_name,
            Site(
                location=region,
                kind="functionapp",
                server_farm_id=app_service_plan.id,
                site_config=SiteConfig(
                    app_settings=[
                        NameValuePair(name="FUNCTIONS_WORKER_RUNTIME", value="python"),
                        NameValuePair(name="AzureWebJobsStorage", value=connection_string),
                    ]
                ),
            ),
        )

        _, blob_forwarder_data = await gather(function_app_future.result(), self.get_blob_forwarder_data())

        # deploy code to function app
        # curl -X POST -H "Authorization: Bearer $TOKEN" -T @"<zip-package-path>" "https://<app-name>.scm.azurewebsites.net/api/publish?type=zip"
        resp = await self.rest_client.post(
            f"https://{function_app_name}.scm.azurewebsites.net/api/publish?type=zip",
            data=blob_forwarder_data,
        )
        resp.raise_for_status()

        return {
            "type": "storageaccount",
            "id": log_forwarder_id,
            "storage_account_id": cast(str, storage_account.id),
        }

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

    async def delete_log_forwarder(self, region: str) -> str:
        return NotImplemented


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
        await gather(*(self.process_subscription(sub_id) for sub_id in self.resource_cache))

    async def process_subscription(self, subscription_id: str) -> None:
        previous_region_assignments = set(self._assignment_cache_initial_state[subscription_id].keys())
        current_regions = set(self.resource_cache[subscription_id].keys())
        regions_to_add = current_regions - previous_region_assignments
        regions_to_remove = previous_region_assignments - current_regions

        async with LogForwarderClient(self.credential, subscription_id, self.resource_group) as client:
            tasks: list[Coroutine[Any, Any, None]] = []
            if regions_to_add:
                tasks.extend(self.create_log_forwarder(client, region) for region in regions_to_add)
            if regions_to_remove:
                tasks.extend(self.delete_log_forwarder(client, region) for region in regions_to_remove)
            await gather(*tasks)

        self.update_assignments(subscription_id)

    async def create_log_forwarder(
        self,
        client: LogForwarderClient,
        region: str,
    ) -> None:
        log.info("Creating log forwarder for subscription %s in region %s", client.subscription_id, region)
        configuration = await client.create_log_forwarder(region)
        self.assignment_cache[client.subscription_id][region] = {
            "configurations": {configuration["id"]: configuration},
            "resources": {},
        }

    async def delete_log_forwarder(
        self,
        client: LogForwarderClient,
        region: str,
    ) -> None:
        log.info("Deleting log forwarder for subscription %s in region %s", client.subscription_id, region)
        config_id = await client.delete_log_forwarder(region)
        self.assignment_cache[client.subscription_id][region]["configurations"].pop(config_id, None)

    def update_assignments(self, sub_id: str) -> None:
        for region_config in self.assignment_cache[sub_id].values():
            diagnostic_setting_configurations = region_config["configurations"]
            assert (
                len(diagnostic_setting_configurations) == 1
            )  # TODO(AZINTS-2388) right now we only have one config per region

            # just use the one config we have for now
            config_id = list(diagnostic_setting_configurations.keys())[0]

            resource_assignments = region_config["resources"]

            # update all the resource assignments to use the new config
            for resource_id in resource_assignments:
                resource_assignments[resource_id] = config_id

    async def write_caches(self) -> None:
        if self.assignment_cache == self._assignment_cache_initial_state:
            log.info("Assignments have not changed, no update needed")
            return
        await write_cache(ASSIGNMENT_CACHE_BLOB, dumps(self.assignment_cache))
        log.info("Updated assignments stored in the cache")


async def main() -> None:
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
