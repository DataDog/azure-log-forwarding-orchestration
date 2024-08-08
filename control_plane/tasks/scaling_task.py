# stdlib
from asyncio import Task as AsyncTask
from asyncio import create_task, gather, run, wait
from collections.abc import Coroutine
from copy import deepcopy
from datetime import datetime, timedelta
from json import dumps
from logging import DEBUG, INFO, basicConfig, getLogger
from os import environ
from typing import Any
from uuid import uuid4

# 3p
from azure.core.exceptions import HttpResponseError
from tenacity import RetryError, retry, retry_if_result, stop_after_attempt

# project
from cache.assignment_cache import ASSIGNMENT_CACHE_BLOB, deserialize_assignment_cache
from cache.common import (
    DiagnosticSettingType,
    InvalidCacheError,
    get_config_option,
    read_cache,
    write_cache,
)
from cache.metric_blob_cache import MetricBlobEntry, deserialize_blob_metric_dict
from cache.resources_cache import RESOURCE_CACHE_BLOB, deserialize_resource_cache
from tasks.client.log_forwarder_client import LogForwarderClient
from tasks.task import Task, now

SCALING_TASK_NAME = "scaling_task"

SHOULD_SUBMIT_METRICS = environ.get("SHOULD_SUBMIT_METRICS", False)
METRIC_COLLECTION_PERIOD_MINUTES = 30
FORWARDER_METRIC_CONTAINER_NAME = "forwarder-metrics"

SCALE_UP_EXECUTION_SECONDS = 25
SCALE_DOWN_EXECUTION_SECONDS = 3

log = getLogger(SCALING_TASK_NAME)
log.setLevel(DEBUG)


class ScalingTask(Task):
    def __init__(self, resource_cache_state: str, assignment_cache_state: str, resource_group: str) -> None:
        super().__init__()
        self.resource_group = resource_group

        self.background_tasks: set[AsyncTask[Any]] = set()

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

    def submit_background_task(self, coro: Coroutine[Any, Any, Any]) -> None:
        def _done_callback(task: AsyncTask[Any]) -> None:
            self.background_tasks.discard(task)
            if e := task.exception():
                log.error("Background task failed with an exception", exc_info=e)

        task = create_task(coro)
        self.background_tasks.add(task)
        task.add_done_callback(_done_callback)

    async def run(self) -> None:
        log.info("Running for %s subscriptions: %s", len(self.resource_cache), list(self.resource_cache.keys()))
        all_subscriptions = set(self.resource_cache.keys()) | set(self.assignment_cache.keys())
        await gather(*(self.process_subscription(sub_id) for sub_id in all_subscriptions))

    async def process_subscription(self, subscription_id: str) -> None:
        previous_region_assignments = set(self._assignment_cache_initial_state.get(subscription_id, {}).keys())
        current_regions = set(self.resource_cache.get(subscription_id, {}).keys())
        regions_to_add = current_regions - previous_region_assignments
        regions_to_remove = previous_region_assignments - current_regions
        regions_to_check_scaling = current_regions & previous_region_assignments
        async with LogForwarderClient(self.credential, subscription_id, self.resource_group) as client:
            await gather(
                *(self.set_up_region(client, subscription_id, region) for region in regions_to_add),
                *(self.delete_region(client, subscription_id, region) for region in regions_to_remove),
                *(self.scale_region(client, subscription_id, region) for region in regions_to_check_scaling),
            )

            if self.background_tasks:
                await wait(self.background_tasks)

    @retry(stop=stop_after_attempt(3), retry=retry_if_result(lambda result: result is None))
    async def create_log_forwarder(
        self, client: LogForwarderClient, region: str
    ) -> tuple[str, DiagnosticSettingType] | None:
        """Creates a log forwarder for the given subscription and region and returns the configuration id and type.
        Will try 3 times, and if the creation fails, the forwarder is (attempted to be) deleted and None is returned"""
        config_id = str(uuid4())[-12:]  # take the last section since we are length limited
        try:
            config_type = await client.create_log_forwarder(region, config_id)
            return config_id, config_type
        except Exception:
            log.exception("Failed to create log forwarder %s, cleaning up", config_id)
            success = await client.delete_log_forwarder(config_id, raise_error=False)
            if not success:
                log.error("Failed to clean up log forwarder %s, manual intervention required", config_id)
            return None

    async def set_up_region(
        self,
        client: LogForwarderClient,
        subscription_id: str,
        region: str,
    ) -> None:
        """Creates a log forwarder for the given subscription and region and assigns resources to it.
        Only done the first time we discover a new region."""
        log.info("Creating log forwarder for subscription %s in region %s", subscription_id, region)
        result = await self.create_log_forwarder(client, region)
        if result is None:
            return
        config_id, config_type = result
        self.assignment_cache.setdefault(subscription_id, {})[region] = {
            "configurations": {config_id: config_type},
            "resources": {resource: config_id for resource in self.resource_cache[subscription_id][region]},
        }

    async def delete_region(
        self,
        client: LogForwarderClient,
        subscription_id: str,
        region: str,
    ) -> None:
        """Cleans up a region by deleting all log forwarders for the given subscription and region."""
        log.info("Deleting log forwarder for subscription %s in region %s", subscription_id, region)
        await gather(
            *(
                client.delete_log_forwarder(forwarder_id)
                for forwarder_id in self.assignment_cache[subscription_id][region]["configurations"]
            )
        )
        del self.assignment_cache[subscription_id][region]

    async def scale_region(
        self,
        client: LogForwarderClient,
        subscription_id: str,
        region: str,
    ) -> None:
        """Checks the performance/scaling of a region and determines/performs scaling as needed

        Additionally assigns new resources to the least busy forwarder
        and reassigns resources based on the new scaling"""
        log.info("Checking scaling for log forwarders in region %s", region)
        region_configs = self.assignment_cache[subscription_id][region]["configurations"]
        metrics = await gather(*(self.collect_forwarder_metrics(config_id, client) for config_id in region_configs))
        forwarder_metrics = dict(zip(region_configs, metrics, strict=False))

        self.onboard_new_resources(subscription_id, region, forwarder_metrics)

        underscaled_forwarders = [
            config_id
            for config_id, _ in forwarder_metrics.items()
            if True  # TODO (AZINTS-2684) implement proper thresholds based on multiple metrics
        ]
        if not underscaled_forwarders:
            # TODO (AZINTS-2389) implement scaling down
            # if we don't scale up and are good to scale down
            return

        new_forwarders = await gather(*[self.create_log_forwarder(client, region) for _ in underscaled_forwarders])

        for underscaled_forwarder_id, new_forwarder in zip(underscaled_forwarders, new_forwarders, strict=False):
            if not new_forwarder:
                log.warning("Failed to create new log forwarder, skipping scaling for %s", underscaled_forwarder_id)
                continue
            self.split_forwarder_resources(subscription_id, region, underscaled_forwarder_id, *new_forwarder)

    def onboard_new_resources(
        self, subscription_id: str, region: str, forwarder_metrics: dict[str, list[MetricBlobEntry]]
    ) -> None:
        """Assigns new resources to the least busy forwarder in the region"""
        new_resources = set(self.resource_cache[subscription_id][region]) - set(
            self.assignment_cache[subscription_id][region]["resources"]
        )
        if not new_resources or not forwarder_metrics:
            return

        # any forwarders without metrics we should not add more resources to, there may be something wrong
        least_busy_forwarder_id = "TODO"  # TODO (AZINTS-2684) implement proper min scaling based on new metrics
        # least_busy_forwarder_id, _ = min(
        #     forwarder_metrics.items(), key=lambda pair: pair[1][0]
        # )

        self.assignment_cache[subscription_id][region]["resources"].update(
            {resource: least_busy_forwarder_id for resource in new_resources}
        )

    def split_forwarder_resources(
        self,
        subscription_id: str,
        region: str,
        underscaled_forwarder_id: str,
        new_config_id: str,
        new_config_type: DiagnosticSettingType,
    ) -> None:
        """Splits the resources of an underscaled forwarder between itself and a new forwarder"""

        # add new config
        self.assignment_cache[subscription_id][region]["configurations"][new_config_id] = new_config_type

        # split resources in half
        assigned_resources = sorted(
            resource_id
            for resource_id, config_id in self.assignment_cache[subscription_id][region]["resources"].items()
            if config_id == underscaled_forwarder_id
        )
        split_index = len(assigned_resources) // 2
        self.assignment_cache[subscription_id][region]["resources"].update(
            {
                **{resource: underscaled_forwarder_id for resource in assigned_resources[:split_index]},
                **{resource: new_config_id for resource in assigned_resources[split_index:]},
            }
        )

    async def collect_forwarder_metrics(self, config_id: str, client: LogForwarderClient) -> list[MetricBlobEntry]:
        """Collects metrics for a given forwarder and submits them to the metrics endpoint"""
        try:
            forwarder_metrics: list[MetricBlobEntry] = []
            metric_dicts = await client.get_blob_metrics(config_id, FORWARDER_METRIC_CONTAINER_NAME)
            oldest_time: datetime = datetime.now() - timedelta(minutes=METRIC_COLLECTION_PERIOD_MINUTES)
            forwarder_metrics = [
                metric_entry
                for metric_str in metric_dicts
                if (metric_entry := deserialize_blob_metric_dict(metric_str, oldest_time.timestamp()))
            ]
            if not forwarder_metrics:
                log.warning("No valid metrics found for forwarder %s", config_id)
            if SHOULD_SUBMIT_METRICS:
                self.submit_background_task(client.submit_log_forwarder_metrics(config_id, forwarder_metrics))
            return forwarder_metrics
        except HttpResponseError:
            log.exception("Recieved azure HTTP error: ")
            return forwarder_metrics
        except RetryError:
            log.error("Max retries attempted")
            return forwarder_metrics

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
