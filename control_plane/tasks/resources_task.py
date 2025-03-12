# stdlib
from asyncio import gather, run
from json import dumps
from os import getenv
from typing import cast

# 3p
from azure.mgmt.resource.subscriptions.v2021_01_01.aio import SubscriptionClient

# project
from cache.common import write_cache
from cache.env import MONITORED_SUBSCRIPTIONS_SETTING, RESOURCE_TAG_FILTER_SETTING
from cache.resources_cache import (
    RESOURCE_CACHE_BLOB,
    ResourceCache,
    deserialize_monitored_subscriptions,
    deserialize_resource_cache,
    prune_resource_cache,
)
from tasks.client.resource_client import ResourceClient
from tasks.task import Task, task_main

RESOURCES_TASK_NAME = "resources_task"


class ResourcesTask(Task):
    NAME = RESOURCES_TASK_NAME

    def __init__(self, resource_cache_state: str) -> None:
        super().__init__()
        self.monitored_subscriptions = deserialize_monitored_subscriptions(
            getenv(MONITORED_SUBSCRIPTIONS_SETTING) or ""
        )
        resource_cache = deserialize_resource_cache(resource_cache_state)
        if resource_cache is None:
            self.log.warning("Resource Cache is in an invalid format, task will reset the cache")
            resource_cache = {}
        self._resource_cache_initial_state = resource_cache

        self.resource_cache: ResourceCache = {}
        "in-memory cache of subscription_id to resource_ids"

        self.set_resource_tag_filters(getenv(RESOURCE_TAG_FILTER_SETTING, ""))

    def set_resource_tag_filters(self, tag_filter_input: str):
        self.inclusive_tags = []
        self.excluding_tags = []
        if len(tag_filter_input) == 0:
            return

        parsed_tags = tag_filter_input.split(",")

        for parsed_tag in parsed_tags:
            tag = parsed_tag.strip().casefold()
            if tag.startswith("!"):
                self.excluding_tags.append(tag[1:])
            else:
                self.inclusive_tags.append(tag)

    async def run(self) -> None:
        async with SubscriptionClient(self.credential) as subscription_client:
            subscriptions = [
                cast(str, sub.subscription_id).lower() async for sub in subscription_client.subscriptions.list()
            ]

        self.log.info("Found %s subscriptions", len(subscriptions))

        if self.monitored_subscriptions is not None:
            all_subscription_count = len(subscriptions)
            subscriptions = [sub for sub in subscriptions if sub in self.monitored_subscriptions]
            self.log.info(
                "Filtered %s subscriptions down to the monitored subscriptions list (%s subscriptions)",
                all_subscription_count,
                len(subscriptions),
            )

        await gather(*map(self.process_subscription, subscriptions))

    async def process_subscription(self, subscription_id: str) -> None:
        self.log.debug("Processing the following subscription: %s", subscription_id)
        async with ResourceClient(
            self.log,
            self.credential,
            self.inclusive_tags,
            self.excluding_tags,
            subscription_id,
        ) as client:
            self.resource_cache[subscription_id] = await client.get_resources_per_region()

    async def write_caches(self) -> None:
        prune_resource_cache(self.resource_cache)

        subscription_count = len(self.resource_cache)
        region_count = sum(len(regions) for regions in self.resource_cache.values())
        resources_count = sum(
            len(resources) for regions in self.resource_cache.values() for resources in regions.values()
        )

        if self.resource_cache == self._resource_cache_initial_state:
            self.log.info("Resources have not changed, no update needed to %s resources", resources_count)
            return

        # since sets cannot be json serialized, we convert them to lists before storing
        await write_cache(RESOURCE_CACHE_BLOB, dumps(self.resource_cache, default=list))

        self.log.info(
            "Updated Resources, monitoring %s resources stored in the cache across %s regions across %s subscriptions",
            resources_count,
            region_count,
            subscription_count,
        )


async def main() -> None:
    await task_main(ResourcesTask, [RESOURCE_CACHE_BLOB])


if __name__ == "__main__":  # pragma: no cover
    run(main())
