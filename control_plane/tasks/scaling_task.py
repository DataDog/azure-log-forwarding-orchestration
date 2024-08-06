# stdlib
from asyncio import Task as AsyncTask
from asyncio import create_task, gather, run, wait
from collections.abc import Coroutine
from copy import deepcopy
from json import dumps
from logging import DEBUG, INFO, basicConfig, getLogger
from os import environ
from typing import Any, TypeAlias
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
    get_function_app_id,
    read_cache,
    write_cache,
)
from cache.resources_cache import RESOURCE_CACHE_BLOB, deserialize_resource_cache
from tasks.client.log_forwarder_client import COLLECTED_METRIC_DEFINITIONS, LogForwarderClient
from tasks.task import Task, now

SCALING_TASK_NAME = "scaling_task"

SHOULD_SUBMIT_METRICS = environ.get("SHOULD_SUBMIT_METRICS", False)

SCALE_UP_EXECUTION_SECONDS = 25
SCALE_DOWN_EXECUTION_SECONDS = 3

log = getLogger(SCALING_TASK_NAME)
log.setLevel(DEBUG)

Metrics: TypeAlias = dict[str, float]
"Mapping of metric name to metric value"


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
            tasks.extend(self.set_up_region(client, subscription_id, region) for region in regions_to_add)
            tasks.extend(
                self.delete_region_log_forwarders(client, subscription_id, region) for region in regions_to_remove
            )
            tasks.extend(self.scale_region(client, subscription_id, region) for region in regions_to_check_scaling)

            await gather(*tasks)
            if self.background_tasks:
                await wait(self.background_tasks)

    @retry(stop=stop_after_attempt(3), retry=retry_if_result(lambda result: result is None))
    async def create_log_forwarder(
        self, client: LogForwarderClient, region: str
    ) -> tuple[str, DiagnosticSettingType] | None:
        """Creates a log forwarder for the given subscription and region and returns the configuration id and type"""
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
        log.info("Creating log forwarder for subscription %s in region %s", subscription_id, region)
        result = await self.create_log_forwarder(client, region)
        if result is None:
            return
        config_id, config_type = result
        self.assignment_cache.setdefault(subscription_id, {})[region] = {
            "configurations": {config_id: config_type},
            "resources": {resource: config_id for resource in self.resource_cache[subscription_id][region]},
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

    async def scale_region(
        self,
        client: LogForwarderClient,
        subscription_id: str,
        region: str,
    ) -> None:
        log.info("Checking scaling for log forwarders in region %s", region)
        region_configs = self.assignment_cache[subscription_id][region]["configurations"]
        forwarder_metrics = await gather(
            *(
                self.collect_forwarder_metrics(config_id, subscription_id, client)
                for config_id, config_type in region_configs.items()
            )
        )

        underscaled_forwarders = [
            config_id
            for config_id, metrics in zip(region_configs, forwarder_metrics)
            if metrics.get("function_execution_time", 0) > SCALE_UP_EXECUTION_SECONDS
        ]
        if not underscaled_forwarders:
            # TODO (AZINTS-2389) implement scaling down
            # if we don't scale up and are good to scale down
            return

        new_forwarders = await gather(*[self.create_log_forwarder(client, region) for _ in underscaled_forwarders])

        for underscaled_forwarder, new_forwarder in zip(underscaled_forwarders, new_forwarders):
            if new_forwarder is None:
                log.warning("Failed to create new log forwarder, skipping scaling for %s", underscaled_forwarder)
                continue
            new_config_id, new_config_type = new_forwarder
            # add new config
            region_configs[new_config_id] = new_config_type

            # split resources in half
            assigned_resources = sorted(
                r
                for r, config_id in self.assignment_cache[subscription_id][region]["resources"].items()
                if config_id == underscaled_forwarder
            )
            split_index = len(assigned_resources) // 2
            self.assignment_cache[subscription_id][region]["resources"].update(
                {
                    **{resource: new_config_id for resource in assigned_resources[split_index:]},
                    **{resource: underscaled_forwarder for resource in assigned_resources[:split_index]},
                }
            )

    async def collect_forwarder_metrics(self, config_id: str, sub_id: str, client: LogForwarderClient) -> Metrics:
        """Updates the log_forwarder_metric_cache entry for a log forwarder
        If there is an error the entry is set to an empty dict"""
        metric_dict = {}
        try:
            response = await client.get_log_forwarder_metrics(
                get_function_app_id(sub_id, self.resource_group, config_id)
            )

            for metric in response.metrics:
                log.debug(metric.name)
                log.debug(metric.unit)
                min_metric_val = None
                max_metric_val = None
                for time_series_element in metric.timeseries:
                    for metric_value in time_series_element.data:
                        log.debug(metric_value.timestamp)
                        log.debug(
                            f"{metric.name}: {COLLECTED_METRIC_DEFINITIONS.get(metric.name, '')} = {getattr(metric_value, COLLECTED_METRIC_DEFINITIONS.get(metric.name, ''), None)}"
                        )
                        metric_val = getattr(metric_value, COLLECTED_METRIC_DEFINITIONS.get(metric.name, ""), None)
                        if metric_val is None:
                            log.info(metric_value.__dict__)
                            log.warning(f"{metric.name} is None for log forwarder: {config_id}. Skipping resource...")
                            return {}
                        if min_metric_val:
                            min_metric_val = min(min_metric_val, metric_val)
                            max_metric_val = max(max_metric_val, metric_val)
                        else:
                            min_metric_val, max_metric_val = metric_val, metric_val
                if max_metric_val is not None:
                    metric_dict[metric.name] = max_metric_val
            if SHOULD_SUBMIT_METRICS:
                task = create_task(client.submit_log_forwarder_metrics(config_id, response.metrics, sub_id))
                self.background_tasks.add(task)
                task.add_done_callback(self.background_tasks.discard)
            return metric_dict
        except HttpResponseError as err:
            log.error(err)
            return {}
        except RetryError:
            log.error("Max retries attempted")
            return {}

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
