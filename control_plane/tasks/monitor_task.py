# stdlib
from asyncio import gather
import asyncio
from datetime import timedelta
from json import dumps, loads
from logging import ERROR, INFO, basicConfig, getLogger
from typing import Self
from tasks.task import now
from tenacity import RetryError, retry, retry_if_exception_type, stop_after_attempt

# 3p
from azure.core.exceptions import HttpResponseError, ServiceResponseTimeoutError
from azure.monitor.query.aio import MetricsQueryClient
from azure.monitor.query import MetricsQueryResult

# project
from cache.diagnostic_settings_cache import (
    deserialize_diagnostic_settings_cache,
)
from cache.resource_metric_cache import ResourceMetricCache
from tasks.task import Task


# silence azure logging except for errors
getLogger("azure").setLevel(ERROR)


MONITOR_TASK_NAME = "monitor_task"
COLLECTED_METRIC_DEFINITIONS = {"FunctionExecutionCount": "total"}

METRIC_COLLECTION_PERIOD = 120  # How long we are mointoring in minutes
METRIC_COLLECTION_SAMPLES = 8  # Number of samples we are collecting
METRIC_COLLECTION_GRANULARITY = METRIC_COLLECTION_PERIOD // METRIC_COLLECTION_SAMPLES

CLIENT_MAX_SECONDS_PER_METRIC = 5
CLIENT_MAX_SECONDS = CLIENT_MAX_SECONDS_PER_METRIC * len(COLLECTED_METRIC_DEFINITIONS)
MAX_ATTEMPS = 5

log = getLogger(MONITOR_TASK_NAME)


class MonitorTask(Task):
    def __init__(self, assignment_cache_state: str) -> None:
        super().__init__()

        # read caches

        success, assignment_settings_cache = deserialize_diagnostic_settings_cache(assignment_cache_state)
        if not success:
            log.warning("Assignments Cache is in an invalid format, resetting the cache")
            assignment_settings_cache = {}

        self.assignment_settings_cache = assignment_settings_cache
        # TODO(<Dan-Nedelescu>): Fix once #20 gets merged
        self.assignment_settings_cache = loads(assignment_cache_state)
        self.resource_metric_cache: ResourceMetricCache = {}
        self.client = MetricsQueryClient(self.credential)

        # define metrics

        self.metric_defs = COLLECTED_METRIC_DEFINITIONS
        self.max_query_time = CLIENT_MAX_SECONDS

    async def __aenter__(self) -> Self:
        await super().__aenter__()
        await self.client.__aenter__()
        return self

    async def __aexit__(self, *_) -> None:
        await self.client.__aexit__()
        await super().__aexit__()

    async def run(self) -> None:
        log.info("Crawling %s subscriptions", len(self.assignment_settings_cache))
        await gather(*[self.process_subscription(sub_id) for sub_id in self.assignment_settings_cache.keys()])

    async def process_subscription(self, sub_id: str):
        resources_per_region = self.assignment_settings_cache[sub_id]
        await gather(
            *[
                self.process_resource(resource_id)
                for region_name, region_data in resources_per_region.items()
                for resource_name, resource_id in region_data["resources"].items()  # type: ignore
            ]
        )

    """ Updates the resource_metric_cache entry for a resource
     If there is an error the entry is set to an empty dict"""

    async def process_resource(self, resource_id: str) -> None:
        metric_dict: dict[str, int | float] = dict()
        try:
            response = await self.get_resource_metrics(resource_id)

            for metric in response.metrics:
                log.debug(metric.name)
                log.debug(metric.unit)
                metric_vals = [None, None]  # (Min,Max)
                for time_series_element in metric.timeseries:
                    for metric_value in time_series_element.data:
                        log.debug(metric_value.timestamp)
                        log.debug(
                            f"{metric.name}: {self.metric_defs[metric.name]} = {getattr(metric_value, self.metric_defs[metric.name])}"
                        )
                        metric_val = getattr(metric_value, self.metric_defs[metric.name])
                        if not metric_val:
                            log.warn(f"{metric.name} is None for resource: {resource_id}. Skipping resource...")
                            self.resource_metric_cache[resource_id] = dict()
                            return
                        if metric_vals[0]:
                            metric_vals[0] = min(metric_vals[0], metric_val)
                            metric_vals[1] = max(metric_vals[1], metric_val)
                        else:
                            metric_vals = [metric_val, metric_val]
                if metric_vals[1]:
                    metric_dict[metric.name] = metric_vals[1]
            self.resource_metric_cache[resource_id] = metric_dict if metric_dict else dict()
        except HttpResponseError as err:
            log.error(err)
            self.resource_metric_cache[resource_id] = dict()
        except RetryError:
            log.error("Max retries attempted")
            self.resource_metric_cache[resource_id] = dict()

    @retry(retry=retry_if_exception_type(ServiceResponseTimeoutError), stop=stop_after_attempt(MAX_ATTEMPS))
    async def get_resource_metrics(self, resource_id: str) -> MetricsQueryResult:
        response = await self.client.query_resource(
            resource_id,
            metric_names=list(self.metric_defs.keys()),
            timespan=timedelta(minutes=METRIC_COLLECTION_PERIOD),
            granularity=timedelta(minutes=METRIC_COLLECTION_GRANULARITY),
            timeout=self.max_query_time,
        )
        return response

    async def write_caches(self) -> None:
        log.info("Output_dict: " + str(self.resource_metric_cache))


async def main():
    basicConfig(level=INFO)
    log.info("Started task at %s", now())
    # This is holder code until assignment cache becomes availaible
    resources = dumps(
        {
            "sub_id1": {
                "EAST_US": {
                    "resources": {
                        "diagnostic-settings-task": "subscriptions/0b62a232-b8db-4380-9da6-640f7272ed6d/resourceGroups/lfo/providers/Microsoft.Web/sites/resources-task"
                    },
                    "configurations": {
                        "OLD_LOG_FORWARDER_ID": {
                            "type": "storageaccount",
                            "id": "OLD_LOG_FORWARDER_ID",
                            "storage_account_id": "some/storage/account",
                        },
                    },
                }
            },
        }
    )
    async with MonitorTask(resources) as task:
        await task.run()
    log.info("Task finished at %s", now())


if __name__ == "__main__":
    asyncio.run(main())
