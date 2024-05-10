# stdlib
from asyncio import gather
import asyncio
from datetime import datetime
from json import dumps
from logging import INFO, getLogger
from typing import cast


# 3p
from azure.mgmt.resource.subscriptions.v2021_01_01.aio import SubscriptionClient
from azure.mgmt.resource.resources.v2021_01_01.aio import ResourceManagementClient

from cache.resources_cache import RESOURCE_CACHE_BLOB, ResourceCache, deserialize_resource_cache
from tasks.common import Task, read_cache, write_cache


RESOURCES_TASK_NAME = "resources_task"
BLOB_STORAGE_CACHE = "resources-cache"
STORAGE_CONNECTION_SETTING = "AzureWebJobsStorage"


log = getLogger(RESOURCES_TASK_NAME)
log.setLevel(INFO)


class ResourcesTask(Task):
    def __init__(self, resource_cache_state: str) -> None:
        super().__init__()
        success, resource_cache = deserialize_resource_cache(resource_cache_state)
        if not success:
            log.warning("Resource Cache is in an invalid format, task will reset the cache")
            resource_cache = {}
        self._resource_cache_initial_state = resource_cache

        self.resource_cache: ResourceCache = {}
        "in-memory cache of subscription_id to resource_ids"

    async def run(self) -> None:
        async with SubscriptionClient(self.credential) as subscription_client:
            await gather(
                *[
                    self.process_subscription(cast(str, sub.subscription_id))
                    async for sub in subscription_client.subscriptions.list()
                ]
            )

    async def process_subscription(self, subscription_id: str) -> None:
        log.info("Processing the following subscription: %s", subscription_id)
        async with ResourceManagementClient(self.credential, subscription_id) as client:
            resource_ids: set[str] = {cast(str, r.id) async for r in client.resources.list()}
            log.info(f"Subscription {subscription_id}: Collected {len(resource_ids)} resources")
            self.resource_cache[subscription_id] = resource_ids

    async def write_caches(self) -> None:
        if self.resource_cache != self._resource_cache_initial_state:
            # since sets cannot be json serialized, we convert them to lists before storing
            await write_cache(RESOURCE_CACHE_BLOB, dumps(self.resource_cache, default=list))
            resources_count = sum(len(resources) for resources in self.resource_cache.values())
            log.info(f"Updated Resources, {resources_count} resources stored in the cache")
        else:
            log.info("Resources have not changed, no update needed")


def now() -> str:
    return datetime.now().isoformat()


async def main() -> None:
    log.info("Started task at %s", now())
    resources_cache_state = await read_cache(RESOURCE_CACHE_BLOB)
    async with ResourcesTask(resources_cache_state) as task:
        await task.run()
    log.info("Task finished at %s", now())


if __name__ == "__main__":
    asyncio.run(main())
