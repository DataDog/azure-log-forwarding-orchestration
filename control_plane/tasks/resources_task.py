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
from cache.env import (
    CONTROL_PLANE_REGION_SETTING,
    MONITORED_SUBSCRIPTIONS_SETTING,
    RESOURCE_PROVIDER_CONFIGS_SETTING,
    RESOURCE_TAG_FILTERS_SETTING,
    get_config_option,
)
from cache.resources_cache import (
    RESOURCE_CACHE_BLOB,
    ResourceCache,
    ResourceProviderConfigs,
    deserialize_monitored_subscriptions,
    deserialize_resource_cache,
    deserialize_resource_provider_configs,
    deserialize_resource_tag_filters,
    prune_resource_cache,
)
from tasks.client.datadog_api_client import StatusCode
from tasks.client.resource_client import ResourceClient
from tasks.common import get_azure_mgmt_url
from tasks.task import Task, task_main

RESOURCES_TASK_NAME = "resources_task"


class ResourcesTask(Task):
    NAME = RESOURCES_TASK_NAME

    def __init__(self, resource_cache_state: str, execution_id: str = "", is_initial_run: bool = False) -> None:
        super().__init__(is_initial_run=is_initial_run, execution_id=execution_id)
        self.base_url = get_azure_mgmt_url(get_config_option(CONTROL_PLANE_REGION_SETTING))
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

        resource_provider_configs_str = getenv(RESOURCE_PROVIDER_CONFIGS_SETTING) or "{}"
        self.resource_provider_configs = deserialize_resource_provider_configs(resource_provider_configs_str)
        """None when the configured value is invalid, in which case the resource cache is left unchanged,
        since ignoring the configs could remove diagnostic settings from every resource"""
        if self.resource_provider_configs is None:
            self.log.error(
                "Invalid value for %s, resource cache will not be updated: %s",
                RESOURCE_PROVIDER_CONFIGS_SETTING,
                resource_provider_configs_str,
            )

    async def run(self) -> None:
        await self.submit_status_update("task_start", StatusCode.OK, "Resources task started")

        if self.schema_upgrade:
            self.log.warning("Detected resource cache schema upgrade, flushing cache")
            await self.write_caches(is_schema_upgrade=True)
            self.schema_upgrade = False

        resource_provider_configs = self.resource_provider_configs
        if resource_provider_configs is None:
            await self.submit_status_update(
                "resource_provider_configs",
                StatusCode.DESERIALIZATION_ERROR,
                f"Invalid value for {RESOURCE_PROVIDER_CONFIGS_SETTING}, resource cache was not updated",
            )
            return

        if resource_provider_configs.overrides or not resource_provider_configs.logs_enabled_default:
            self.log.info(
                "Filtering resources by resource provider, logs enabled by default: %s, overrides: %s",
                resource_provider_configs.logs_enabled_default,
                resource_provider_configs.overrides,
            )

        async with SubscriptionClient(self.credential, base_url=self.base_url) as subscription_client:
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

        await gather(*(self.process_subscription(sub, resource_provider_configs) for sub in subscriptions))
        await self.submit_status_update("task_complete", StatusCode.OK, "Resources task completed")

    async def process_subscription(
        self, subscription_id: str, resource_provider_configs: ResourceProviderConfigs
    ) -> None:
        self.log.debug("Processing the following subscription: %s", subscription_id)
        async with ResourceClient(
            self.log,
            self.credential,
            self.tag_filter_list,
            subscription_id,
            self.base_url,
            resource_provider_configs=resource_provider_configs.overrides,
            is_logs_enabled_default=resource_provider_configs.logs_enabled_default,
        ) as client:
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

        if self.resource_provider_configs is None:
            # run() skipped collecting resources, writing the empty in-memory cache would drop every resource
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
