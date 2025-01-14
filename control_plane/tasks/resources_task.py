# stdlib
from asyncio import gather, run
from json import dumps
from logging import DEBUG, basicConfig, getLogger
from os import getenv
from typing import Final, cast

# 3p
from azure.mgmt.resource.resources.v2021_01_01.aio import ResourceManagementClient
from azure.mgmt.resource.subscriptions.v2021_01_01.aio import SubscriptionClient

# project
from cache.common import read_cache, write_cache
from cache.resources_cache import (
    RESOURCE_CACHE_BLOB,
    ResourceCache,
    deserialize_monitored_subscriptions,
    deserialize_resource_cache,
    prune_resource_cache,
)
from tasks.common import (
    DIAGNOSTIC_SETTINGS_TASK_PREFIX,
    FORWARDER_MANAGED_ENVIRONMENT_PREFIX,
    RESOURCES_TASK_PREFIX,
    SCALING_TASK_PREFIX,
    now,
)
from tasks.constants import ALLOWED_RESOURCE_TYPES, ALLOWED_STORAGE_ACCOUNT_REGIONS
from tasks.task import Task

RESOURCES_TASK_NAME = "resources_task"

log = getLogger(RESOURCES_TASK_NAME)

# we only need to ignore forwardable resource types here, since others will be filtered by type.
IGNORED_LFO_PREFIXES: Final = frozenset(
    {
        FORWARDER_MANAGED_ENVIRONMENT_PREFIX,
        SCALING_TASK_PREFIX,
        RESOURCES_TASK_PREFIX,
        DIAGNOSTIC_SETTINGS_TASK_PREFIX,
        # STORAGE_ACCOUNT_PREFIX, # TODO (AZINTS-2763): add these in once we implement adding storage accounts
        # CONTROL_PLANE_STORAGE_PREFIX,
    }
)


RESOURCE_QUERY_FILTER: Final = " or ".join(f"resourceType eq '{rt}'" for rt in ALLOWED_RESOURCE_TYPES)


def should_ignore_resource(region: str, resource_type: str, resource_name: str) -> bool:
    """Determines if we should ignore the resource"""
    name = resource_name.lower()
    return (
        # we must be able to put a storage account in the same region
        # https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/diagnostic-settings#destination-limitations
        region.lower() not in ALLOWED_STORAGE_ACCOUNT_REGIONS
        # only certain resource types have diagnostic settings
        or resource_type.lower() not in ALLOWED_RESOURCE_TYPES
        # ignore resources that are managed by the control plane
        or any(name.startswith(prefix) for prefix in IGNORED_LFO_PREFIXES)
    )


class ResourcesTask(Task):
    def __init__(self, resource_cache_state: str) -> None:
        super().__init__()
        self.monitored_subscriptions = deserialize_monitored_subscriptions(getenv("MONITORED_SUBSCRIPTIONS") or "")
        resource_cache = deserialize_resource_cache(resource_cache_state)
        if resource_cache is None:
            log.warning("Resource Cache is in an invalid format, task will reset the cache")
            resource_cache = {}
        self._resource_cache_initial_state = resource_cache

        self.resource_cache: ResourceCache = {}
        "in-memory cache of subscription_id to resource_ids"

    async def run(self) -> None:
        async with SubscriptionClient(self.credential) as subscription_client:
            subscriptions = [
                cast(str, sub.subscription_id).lower() async for sub in subscription_client.subscriptions.list()
            ]

        log.info("Found %s subscriptions", len(subscriptions))

        if self.monitored_subscriptions is not None:
            all_subscription_count = len(subscriptions)
            subscriptions = [sub for sub in subscriptions if sub in self.monitored_subscriptions]
            log.info(
                "Filtered %s subscriptions down to the monitored subscriptions list (%s subscriptions)",
                all_subscription_count,
                len(subscriptions),
            )

        await gather(*map(self.process_subscription, subscriptions))

    async def process_subscription(self, subscription_id: str) -> None:
        log.debug("Processing the following subscription: %s", subscription_id)
        async with ResourceManagementClient(self.credential, subscription_id) as client:
            resources_per_region: dict[str, set[str]] = {}
            resource_count = 0
            async for r in client.resources.list(RESOURCE_QUERY_FILTER):
                region = cast(str, r.location).lower()
                if should_ignore_resource(region, cast(str, r.type), cast(str, r.name)):
                    continue
                resources_per_region.setdefault(region, set()).add(cast(str, r.id).lower())
                resource_count += 1
            log.debug("Subscription %s: Collected %s resources", subscription_id, resource_count)
            self.resource_cache[subscription_id] = resources_per_region

    async def write_caches(self) -> None:
        prune_resource_cache(self.resource_cache)

        subscription_count = len(self.resource_cache)
        region_count = sum(len(regions) for regions in self.resource_cache.values())
        resources_count = sum(
            len(resources) for regions in self.resource_cache.values() for resources in regions.values()
        )

        if self.resource_cache == self._resource_cache_initial_state:
            log.info("Resources have not changed, no update needed to %s resources", resources_count)
            return

        # since sets cannot be json serialized, we convert them to lists before storing
        await write_cache(RESOURCE_CACHE_BLOB, dumps(self.resource_cache, default=list))

        log.info(
            "Updated Resources, monitoring %s resources stored in the cache across %s regions across %s subscriptions",
            resources_count,
            region_count,
            subscription_count,
        )


async def main() -> None:
    basicConfig(level=DEBUG)
    log.info("Started task at %s", now())
    resources_cache_state = await read_cache(RESOURCE_CACHE_BLOB)
    async with ResourcesTask(resources_cache_state) as task:
        await task.run()
    log.info("Task finished at %s", now())


if __name__ == "__main__":  # pragma: no cover
    run(main())
