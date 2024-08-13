# stdlib
from asyncio import create_task, gather, run
from json import dumps
from logging import DEBUG, basicConfig, getLogger
from typing import cast

# 3p
from azure.mgmt.resource.resources.v2021_01_01.aio import ResourceManagementClient
from azure.mgmt.resource.resources.v2021_01_01.models import ProviderResourceType
from azure.mgmt.resource.subscriptions.v2021_01_01.aio import SubscriptionClient

# project
from cache.common import read_cache, write_cache
from cache.resources_cache import RESOURCE_CACHE_BLOB, ResourceCache, deserialize_resource_cache
from tasks.client.log_forwarder_client import ignore_exception_type
from tasks.common import now
from tasks.task import Task

RESOURCES_TASK_NAME = "resources_task"

log = getLogger(RESOURCES_TASK_NAME)


def get_resource_type(resource_id: str) -> str:
    return resource_id.split("/")[6]


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
        log.debug("Processing the following subscription: %s", subscription_id)
        async with ResourceManagementClient(self.credential, subscription_id) as client:
            api_versions_task = create_task(self.fetch_api_versions(client))
            resources_per_region: dict[str, set[str]] = {}
            resource_count = 0
            async for r in client.resources.list():
                region = cast(str, r.location)
                if region == "global":
                    continue
                resources_per_region.setdefault(region, set()).add(cast(str, r.id))
                resource_count += 1
            log.debug("Subscription %s: Collected %s resources", subscription_id, resource_count)
            self.resource_cache[subscription_id] = resources_per_region
            api_versions = await api_versions_task
            await self.confirm_deletions_for_subscription(client, subscription_id, api_versions)

    async def fetch_api_versions(self, client: ResourceManagementClient) -> dict[str, str]:
        """Fetch all the api versions by resource type"""
        return {
            cast(str, resource_type.resource_type): api_version
            async for provider in client.providers.list()
            for resource_type in cast(list[ProviderResourceType], provider.resource_types)
            if (api_version := resource_type.api_versions[0] if resource_type.api_versions else None)
        }

    async def confirm_deletions_for_subscription(
        self, client: ResourceManagementClient, subscription_id: str, api_versions: dict[str, str]
    ) -> None:
        if subscription_id not in self._resource_cache_initial_state:
            # we discovered a new subscription, no need to check for deletions
            return

        await gather(
            *(
                self.confirm_deletion(client, subscription_id, region, deleted_resources, api_versions)
                for region, previous_resources in self.resource_cache.get(subscription_id, {}).items()
                if (
                    deleted_resources := previous_resources
                    - self.resource_cache.get(subscription_id, {}).get(region, set())
                )
            )
        )

    async def confirm_deletion(
        self,
        client: ResourceManagementClient,
        subscription_id: str,
        region: str,
        resources: set[str],
        api_versions: dict[str, str],
    ) -> None:
        """Confirm that the resources are deleted before removing them from the cache"""
        fetched_resources = await gather(
            *(
                ignore_exception_type(Exception, client.resources.get_by_id(resource_id, api_version))
                for resource_id in resources
                if (api_version := api_versions.get(get_resource_type(resource_id)))
            )
        )
        for resource_id, fetched_resource in zip(resources, fetched_resources, strict=False):
            if fetched_resource is not None:
                log.debug(
                    "Subscription %s: Resource %s was not found in the list endpoint, but still exists. Keeping in the cache",
                    subscription_id,
                    resource_id,
                )
                self.resource_cache[subscription_id][region].add(resource_id)

    async def write_caches(self) -> None:
        if self.resource_cache == self._resource_cache_initial_state:
            log.info("Resources have not changed, no update needed")
            return
        # since sets cannot be json serialized, we convert them to lists before storing
        await write_cache(RESOURCE_CACHE_BLOB, dumps(self.resource_cache, default=list))
        resources_count = sum(len(resources) for resources in self.resource_cache.values())
        log.info(f"Updated Resources, {resources_count} resources stored in the cache")


async def main() -> None:
    basicConfig(level=DEBUG)
    log.info("Started task at %s", now())
    resources_cache_state = await read_cache(RESOURCE_CACHE_BLOB)
    async with ResourcesTask(resources_cache_state) as task:
        await task.run()
    log.info("Task finished at %s", now())


if __name__ == "__main__":  # pragma: no cover
    run(main())
