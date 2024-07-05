# stdlib
from asyncio import gather, run
from copy import deepcopy
from json import dumps
from logging import DEBUG, getLogger
from typing import Any, Coroutine, AsyncContextManager, Self
from uuid import UUID

# 3p
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.web.v2023_12_01.aio import WebSiteManagementClient
from azure.mgmt.web.v2023_12_01.models import AppServicePlan, SkuDescription
from azure.mgmt.storage.v2023_05_01.aio import StorageManagementClient
from azure.mgmt.storage.v2023_05_01.models import StorageAccountCreateParameters, Sku

# project
from cache.assignment_cache import ASSIGNMENT_CACHE_BLOB, deserialize_assignment_cache
from cache.common import DiagnosticSettingConfiguration, InvalidCacheError, read_cache, write_cache
from cache.resources_cache import RESOURCE_CACHE_BLOB, deserialize_resource_cache
from tasks.task import Task, get_config_option, now


SCALING_TASK_NAME = "scaling_task"

log = getLogger(SCALING_TASK_NAME)
log.setLevel(DEBUG)


class LogForwarderClient(AsyncContextManager):
    def __init__(self, credential: DefaultAzureCredential, subscription_id: str, resource_group: str) -> None:
        self.web_client = WebSiteManagementClient(credential, subscription_id)
        self.storage_client = StorageManagementClient(credential, subscription_id)
        self.subscription_id = subscription_id
        self.resource_group = resource_group

    async def __aenter__(self) -> Self:
        await gather(self.web_client.__aenter__(), self.storage_client.__aenter__())
        return self

    async def __aexit__(self, *_) -> None:
        await gather(self.web_client.__aexit__(), self.storage_client.__aexit__())
        pass

    async def create_log_forwarder(self, region: str) -> tuple[str, DiagnosticSettingConfiguration]:
        log_forwarder_id = str(UUID())
        # storage account
        storage_account_name = f"log-forwarder-storage-{log_forwarder_id}"
        storage_account_future = await self.storage_client.storage_accounts.begin_create(
            resource_group_name=self.resource_group,
            account_name=storage_account_name,
            parameters=StorageAccountCreateParameters(sku=Sku(name="Standard_LRS"), kind="StorageV2", location=region),
        )
        # app service plan
        app_service_plan_name = f"log-forwarder-plan-{log_forwarder_id}"
        app_service_plan_future = await self.web_client.app_service_plans.begin_create_or_update(
            self.resource_group,
            app_service_plan_name,
            AppServicePlan(
                location=region, sku=SkuDescription(name="Y1", tier="Dynamic", size="Y1", family="Y", capacity=0)
            ),
        )
        try:
            await storage_account_future.result()
            await app_service_plan_future.result()
        except Exception:
            log.exception("Failed to create log forwarder resources")
            raise

        # self.storage_client.blob_containers.create()
        return log_forwarder_id, NotImplemented

    async def delete_log_forwarder(self, region: str) -> str: ...


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
        config_id, configuration = await client.create_log_forwarder(region)
        self.assignment_cache[client.subscription_id][region] = {
            "configurations": {config_id: configuration},
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
