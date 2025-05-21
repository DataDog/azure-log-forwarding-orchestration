# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from asyncio import gather, run
from json import dumps
from os import getenv
from typing import cast

# 3p
from azure.core.exceptions import HttpResponseError
from azure.mgmt.resource.subscriptions.v2021_01_01.aio import SubscriptionClient

# project
from cache.common import write_cache
from cache.env import MONITORED_SUBSCRIPTIONS_SETTING, RESOURCE_TAG_FILTERS_SETTING
from cache.resources_cache import (
    RESOURCE_CACHE_BLOB,
    ResourceCache,
    deserialize_monitored_subscriptions,
    deserialize_resource_cache,
    deserialize_resource_tag_filters,
    prune_resource_cache,
)
from tasks.client.datadog_api_client import StatusCode
from tasks.client.resource_client import ResourceClient
from tasks.task import Task, task_main

RESOURCES_TASK_NAME = "resources_task"


class ResourcesTask(Task):
    NAME = RESOURCES_TASK_NAME

    def __init__(self, resource_cache_state: str, execution_id: str = "", is_initial_run: bool = False) -> None:
        super().__init__(is_initial_run=is_initial_run, execution_id=execution_id)
        self.monitored_subscriptions = deserialize_monitored_subscriptions(
            getenv(MONITORED_SUBSCRIPTIONS_SETTING) or ""
        )
        resource_cache, self.schema_upgrade = deserialize_resource_cache(resource_cache_state)
        if resource_cache is None:
            self.log.warning("Resource Cache is in an invalid format, task will reset the cache")
            resource_cache = {}
        self._resource_cache_initial_state = resource_cache

        self.resource_cache: ResourceCache = {}
        "in-memory cache of subscription_id to resource_ids"

        self.tag_filter_list = deserialize_resource_tag_filters(getenv(RESOURCE_TAG_FILTERS_SETTING, ""))

    async def run(self) -> None:
        await self.submit_status_update("task_start", StatusCode.OK, "Resources task started")

        if self.schema_upgrade:
            self.log.warning("Detected resource cache schema upgrade, flushing cache")
            await self.write_caches(is_schema_upgrade=True)
            self.schema_upgrade = False

        async with SubscriptionClient(self.credential) as subscription_client:
            try:
                subscriptions = [
                    cast(str, sub.subscription_id).lower() async for sub in subscription_client.subscriptions.list()
                ]
            except HttpResponseError as e:
                self.log.error("Failed to list subscriptions")
                await self.submit_status_update(
                    "subscriptions_list", StatusCode.AZURE_RESPONSE_ERROR, f"Failed to list subscriptions. Reason: {e}"
                )
                return

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
        await self.submit_status_update("task_complete", StatusCode.OK, "Resources task completed")

    async def process_subscription(self, subscription_id: str) -> None:
        self.log.debug("Processing the following subscription: %s", subscription_id)
        async with ResourceClient(self.log, self.credential, self.tag_filter_list, subscription_id) as client:
            try:
                self.resource_cache[subscription_id] = await client.get_resources_per_region()
            except HttpResponseError as e:
                self.log.error("Failed to list resources for subscription %s", subscription_id)
                await self.submit_status_update(
                    "resources_list",
                    StatusCode.AZURE_RESPONSE_ERROR,
                    f"Failed to list resources for subscription {subscription_id}. Reason: {e}",
                )
                return

    async def write_caches(self, is_schema_upgrade: bool = False) -> None:
        if is_schema_upgrade:
            await write_cache(RESOURCE_CACHE_BLOB, dumps(self._resource_cache_initial_state))
            return

        prune_resource_cache(self.resource_cache)

        subscription_count = len(self.resource_cache)
        region_count = sum(len(regions) for regions in self.resource_cache.values())
        resources_count = sum(
            len(resources) for regions in self.resource_cache.values() for resources in regions.values()
        )

        if self.resource_cache == self._resource_cache_initial_state:
            self.log.info("Resources have not changed, no update needed to %s resources", resources_count)
            return

        await write_cache(RESOURCE_CACHE_BLOB, dumps(self.resource_cache))

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
