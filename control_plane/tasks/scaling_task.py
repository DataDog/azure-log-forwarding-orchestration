# stdlib
from asyncio import Task as AsyncTask
from asyncio import gather, run, wait
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
from tenacity import RetryError

# project
from cache.assignment_cache import ASSIGNMENT_CACHE_BLOB, deserialize_assignment_cache
from cache.common import (
    InvalidCacheError,
    get_config_option,
    read_cache,
    write_cache,
)
from cache.metric_blob_cache import MetricBlobEntry, validate_blob_metric_dict
from cache.resources_cache import RESOURCE_CACHE_BLOB, deserialize_resource_cache
from tasks.client.log_forwarder_client import LogForwarderClient
from tasks.task import Task, now

SCALING_TASK_NAME = "scaling_task"

SHOULD_SUBMIT_METRICS = environ.get("SHOULD_SUBMIT_METRICS", False)
METRIC_COLLECTION_PERIOD_MINUTES = 30


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
            tasks: list[Coroutine[Any, Any, None]] = []
            tasks.extend(self.create_log_forwarder(client, subscription_id, region) for region in regions_to_add)
            tasks.extend(
                self.delete_region_log_forwarders(client, subscription_id, region) for region in regions_to_remove
            )
            tasks.extend(
                self.check_region_scaling(client, subscription_id, region) for region in regions_to_check_scaling
            )

            await gather(*tasks)
            if len(self.background_tasks) != 0:
                await wait(self.background_tasks)

        self.update_assignments(subscription_id)

    async def create_log_forwarder(
        self,
        client: LogForwarderClient,
        subscription_id: str,
        region: str,
    ) -> None:
        log.info("Creating log forwarder for subscription %s in region %s", subscription_id, region)
        config_id = str(uuid4())[-12:]  # take the last section since we are length limited
        try:
            config_type = await client.create_log_forwarder(region, config_id)
        except Exception:
            log.exception("Failed to create log forwarder %s, cleaning up", config_id)
            success = await client.delete_log_forwarder(config_id, raise_error=False)
            if not success:
                log.error("Failed to clean up log forwarder %s, manual intervention required", config_id)
            return
        self.assignment_cache.setdefault(subscription_id, {})[region] = {
            "configurations": {config_id: config_type},
            "resources": {},
        }

    async def delete_region_log_forwarders(
        self,
        client: LogForwarderClient,
        subscription_id: str,
        region: str,
    ) -> None:
        log.info("Deleting log forwarder for subscription %s in region %s", subscription_id, region)
        await gather(
            *(
                client.delete_log_forwarder(forwarder_id)
                for forwarder_id in self.assignment_cache[subscription_id][region]["configurations"]
            )
        )
        del self.assignment_cache[subscription_id][region]

    async def check_region_scaling(
        self,
        client: LogForwarderClient,
        subscription_id: str,
        region: str,
    ) -> None:
        log.info("Checking scaling for log forwarders in region %s", region)

        forwarder_metrics = await gather(
            *(
                self.collect_forwarder_metrics(config_id, subscription_id, client)
                for config_id, _ in self.assignment_cache[subscription_id][region]["configurations"].items()
            )
        )

        for fmetric in forwarder_metrics:
            log.info(fmetric)

        await gather(*(self.background_tasks))

        # TODO: AZINTS-2388 implement logic to scale the forwarders based on the metrics

    async def collect_forwarder_metrics(
        self, config_id: str, sub_id: str, client: LogForwarderClient
    ) -> list[MetricBlobEntry] | None:
        """Updates the log_forwarder_metric_cache entry for a log forwarder
        If there is an error the entry is set to an empty dict"""
        # TODO Figure out how to get actual connection string + container name
        try:
            forwarder_metrics: list[MetricBlobEntry] | None = None
            metric_dicts = await client.get_blob_metrics(
                get_config_option("TEST_CONNECTION_STR"), "insights-logs-functionapplogs"
            )
            oldest_time: datetime = datetime.now() - timedelta(minutes=METRIC_COLLECTION_PERIOD_MINUTES)
            forwarder_metrics = [
                metric_list
                for metric_list in [
                    validate_blob_metric_dict(metric_entry, oldest_time.timestamp()) for metric_entry in metric_dicts
                ]
                if metric_list is not None
            ]
            if len(forwarder_metrics) == 0:
                log.info("No metrics found")
                return None
            return forwarder_metrics
        except HttpResponseError:
            log.exception("Recieved azure HTTP error: ")
            return forwarder_metrics
        except RetryError:
            log.error("Max retries attempted")
            return forwarder_metrics

    def update_assignments(self, sub_id: str) -> None:
        for region, region_config in self.assignment_cache[sub_id].items():
            diagnostic_setting_configurations = region_config["configurations"]
            assert (
                len(diagnostic_setting_configurations) == 1
            )  # TODO(AZINTS-2388) right now we only have one config per region

            # just use the one config we have for now
            config_id = list(diagnostic_setting_configurations.keys())[0]

            resource_assignments = region_config["resources"]

            # update all the resource assignments to use the new config
            for resource_id in self.resource_cache[sub_id][region]:
                resource_assignments[resource_id] = config_id

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
