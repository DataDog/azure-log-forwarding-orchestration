# stdlib
from asyncio import gather, run
from json import dumps
from logging import DEBUG, basicConfig, getLogger
from typing import Final, cast

# 3p
from azure.mgmt.resource.resources.v2021_01_01.aio import ResourceManagementClient
from azure.mgmt.resource.subscriptions.v2021_01_01.aio import SubscriptionClient

# project
from cache.common import InvalidCacheError, get_config_option, read_cache, write_cache
from cache.resources_cache import (
    RESOURCE_CACHE_BLOB,
    ResourceCache,
    deserialize_monitored_subscriptions,
    deserialize_resource_cache,
    prune_resource_cache,
)
from tasks.common import (
    DEPLOYER_MANAGED_ENVIRONMENT_NAME,
    DIAGNOSTIC_SETTINGS_TASK_PREFIX,
    FORWARDER_MANAGED_ENVIRONMENT_PREFIX,
    RESOURCES_TASK_PREFIX,
    SCALING_TASK_PREFIX,
    now,
)
from tasks.concurrency import collect
from tasks.constants import ALLOWED_REGIONS, ALLOWED_RESOURCE_TYPES
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
        DEPLOYER_MANAGED_ENVIRONMENT_NAME,
        # STORAGE_ACCOUNT_PREFIX, # TODO (AZINTS-2763): add these in once we implement adding storage accounts
        # CONTROL_PLANE_STORAGE_PREFIX,
    }
)


def should_ignore_resource(region: str, resource_type: str, resource_name: str) -> bool:
    return (
        region not in ALLOWED_REGIONS
        or resource_type not in ALLOWED_RESOURCE_TYPES
        or any(resource_name.startswith(prefix) for prefix in IGNORED_LFO_PREFIXES)
    )


class ResourcesTask(Task):
    def __init__(self, resource_cache_state: str) -> None:
        super().__init__()
        monitored_subs = deserialize_monitored_subscriptions(get_config_option("MONITORED_SUBSCRIPTIONS"))
        if not monitored_subs:
            raise InvalidCacheError("Monitored Subscriptions Must be a valid non-empty list")
        self.monitored_subscriptions = monitored_subs
        resource_cache = deserialize_resource_cache(resource_cache_state)
        if resource_cache is None:
            log.warning("Resource Cache is in an invalid format, task will reset the cache")
            resource_cache = {}
        self._resource_cache_initial_state = resource_cache

        self.resource_cache: ResourceCache = {}
        "in-memory cache of subscription_id to resource_ids"
    
    @staticmethod
    def resource_query_filter(resource_types: set[str]) -> str:
        return " or ".join([f"resourceType eq '{rt}'" for rt in resource_types])

    async def run(self) -> None:
        async with SubscriptionClient(self.credential) as subscription_client:
            subscriptions = await collect(
                sub_id
                async for sub in subscription_client.subscriptions.list()
                if (sub_id := cast(str, sub.subscription_id).casefold()) in self.monitored_subscriptions
            )

        await gather(*map(self.process_subscription, subscriptions))

    async def process_subscription(self, subscription_id: str) -> None:
        log.debug("Processing the following subscription: %s", subscription_id)
        async with ResourceManagementClient(self.credential, subscription_id) as client:
            resources_per_region: dict[str, set[str]] = {}
            resource_count = 0
            resource_filter = ResourcesTask.resource_query_filter(ALLOWED_RESOURCE_TYPES)
            async for r in client.resources.list(resource_filter):
                region = cast(str, r.location).casefold()
                if should_ignore_resource(region, cast(str, r.type).casefold(), cast(str, r.name).casefold()):
                    continue
                resources_per_region.setdefault(region, set()).add(cast(str, r.id).casefold())
                resource_count += 1
            log.debug("Subscription %s: Collected %s resources", subscription_id, resource_count)
            self.resource_cache[subscription_id] = resources_per_region

    async def write_caches(self) -> None:
        prune_resource_cache(self.resource_cache)
        if self.resource_cache == self._resource_cache_initial_state:
            log.info("Resources have not changed, no update needed")
            return
        # since sets cannot be json serialized, we convert them to lists before storing
        await write_cache(RESOURCE_CACHE_BLOB, dumps(self.resource_cache, default=list))

        subscription_count = len(self.resource_cache)
        region_count = sum(len(regions) for regions in self.resource_cache.values())
        resources_count = sum(
            len(resources) for regions in self.resource_cache.values() for resources in regions.values()
        )
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
