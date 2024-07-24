# stdlib
from asyncio import gather
import asyncio
from datetime import timedelta
from json import dumps
from logging import ERROR, INFO, basicConfig, getLogger
from typing import Self
from tasks.task import now
from tenacity import RetryError, retry, retry_if_exception_type, stop_after_attempt

# 3p
from azure.core.exceptions import HttpResponseError, ServiceResponseTimeoutError
from azure.monitor.query.aio import MetricsQueryClient
from azure.monitor.query import MetricsQueryResult

# project
from cache.assignment_cache import deserialize_assignment_cache, AssignmentCache
from cache.common import InvalidCacheError, get_function_app_id
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

        success, assignment_settings_cache = deserialize_assignment_cache(assignment_cache_state)
        if not success:
            log.warning("Assignments Cache is in an invalid format.")
            raise InvalidCacheError("Resource Cache is in an invalid format, failing this task until it is valid")

        self.assignment_settings_cache: AssignmentCache = assignment_settings_cache
        self.resource_metric_cache: ResourceMetricCache = {}
        self.client = MetricsQueryClient(self.credential)

    async def __aenter__(self) -> Self:
        await super().__aenter__()
        await self.client.__aenter__()
        return self

    async def __aexit__(self, *_) -> None:
        await self.client.__aexit__()
        await super().__aexit__()

    async def run(self) -> None:
        log.info("Pulling metrics from %s subscriptions", len(self.assignment_settings_cache))
        await gather(*[self.process_subscription(sub_id) for sub_id in self.assignment_settings_cache.keys()])

    async def process_subscription(self, sub_id: str):
        await gather(
            *[
                self.process_resource(resource_id, sub_id)
                for region_data in self.assignment_settings_cache[sub_id].values()
                for resource_id in region_data["configurations"].keys()
            ]
        )

    async def process_resource(self, resource_id: str, sub_id: str) -> None:
        """Updates the resource_metric_cache entry for a resource
        If there is an error the entry is set to an empty dict"""
        metric_dict = self.resource_metric_cache.setdefault(resource_id, {})
        try:
            response = await self.get_resource_metrics(get_function_app_id(sub_id, "lfo", resource_id))

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
                        if not metric_val:
                            log.warning(f"{metric.name} is None for resource: {resource_id}. Skipping resource...")
                            return
                        if min_metric_val:
                            min_metric_val = min(min_metric_val, metric_val)
                            max_metric_val = max(max_metric_val, metric_val)
                        else:
                            min_metric_val, max_metric_val = metric_val, metric_val
                if max_metric_val:
                    metric_dict[metric.name] = max_metric_val
            self.resource_metric_cache[resource_id] = metric_dict if metric_dict else dict()
        except HttpResponseError as err:
            log.error(err)
        except RetryError:
            log.error("Max retries attempted")

    @retry(retry=retry_if_exception_type(ServiceResponseTimeoutError), stop=stop_after_attempt(MAX_ATTEMPS))
    async def get_resource_metrics(self, resource_id: str) -> MetricsQueryResult:
        return await self.client.query_resource(
            resource_id,
            metric_names=list(COLLECTED_METRIC_DEFINITIONS.keys()),
            timespan=timedelta(minutes=METRIC_COLLECTION_PERIOD),
            granularity=timedelta(minutes=METRIC_COLLECTION_GRANULARITY),
            timeout=CLIENT_MAX_SECONDS,
        )

    async def write_caches(self) -> None:
        log.info("Output_dict: " + str(self.resource_metric_cache))


async def main():
    basicConfig(level=INFO)
    log.info("Started task at %s", now())
    # This is holder code until assignment cache becomes availaible
    resources = dumps(
        {
            "0b62a232-b8db-4380-9da6-640f7272ed6d": {
                "east_us": {
                    "resources": {"diagnostic-settings-task": "32722ff9c26e"},
                    "configurations": {"32722ff9c26e": "storageaccount"},
                }
            },
        }
    )
    try:
        async with MonitorTask(resources) as task:
            await task.run()
    except InvalidCacheError:
        log.warning("Task skipped")
    log.info("Task finished at %s", now())


if __name__ == "__main__":
    asyncio.run(main())
