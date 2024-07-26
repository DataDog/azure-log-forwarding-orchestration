# stdlib
import asyncio
import os
from asyncio import gather
from datetime import datetime, timedelta
from json import dumps
from logging import ERROR, INFO, basicConfig, getLogger
from typing import Self

# 3p
from azure.core.exceptions import HttpResponseError, ServiceResponseTimeoutError
from azure.monitor.query import Metric, MetricsQueryResult, MetricValue
from azure.monitor.query.aio import MetricsQueryClient

# dd
from datadog_api_client import AsyncApiClient, Configuration
from datadog_api_client.v2.api.metrics_api import MetricsApi
from datadog_api_client.v2.model.metric_intake_type import MetricIntakeType
from datadog_api_client.v2.model.metric_payload import MetricPayload
from datadog_api_client.v2.model.metric_point import MetricPoint
from datadog_api_client.v2.model.metric_resource import MetricResource
from datadog_api_client.v2.model.metric_series import MetricSeries
from tenacity import RetryError, retry, retry_if_exception_type, stop_after_attempt

# project
from cache.assignment_cache import AssignmentCache, deserialize_assignment_cache
from cache.common import InvalidCacheError, get_config_option, get_function_app_id, get_function_app_name
from cache.log_forwarder_metric_cache import LogForwarderMetricCache
from tasks.task import Task, now

# silence azure logging except for errors
getLogger("azure").setLevel(ERROR)


MONITOR_TASK_NAME = "monitor_task"
COLLECTED_METRIC_DEFINITIONS = {"FunctionExecutionCount": "total"}

METRIC_COLLECTION_PERIOD = 30  # How long we are mointoring in minutes
METRIC_COLLECTION_SAMPLES = 6  # Number of samples we are collecting
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
        self.log_forwarder_metric_cache: LogForwarderMetricCache = {}
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
                self.process_log_forwarder(config_id, sub_id)
                for region_data in self.assignment_settings_cache[sub_id].values()
                for config_id in region_data["configurations"].keys()
            ]
        )

    async def process_log_forwarder(self, log_forwarder_id: str, sub_id: str) -> None:
        """Updates the log_forwarder_metric_cache entry for a log forwarder
        If there is an error the entry is set to an empty dict"""
        metric_dict = self.log_forwarder_metric_cache.setdefault(log_forwarder_id, {})
        try:
            response = await self.get_log_forwarder_metrics(
                get_function_app_id(sub_id, get_config_option("RESOURCE_GROUP"), log_forwarder_id)
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
                            log.warning(
                                f"{metric.name} is None for log forwarder: {log_forwarder_id}. Skipping resource..."
                            )
                            return
                        if min_metric_val:
                            min_metric_val = min(min_metric_val, metric_val)
                            max_metric_val = max(max_metric_val, metric_val)
                        else:
                            min_metric_val, max_metric_val = metric_val, metric_val
                if max_metric_val is not None:
                    metric_dict[metric.name] = max_metric_val
            if os.environ.get("SHOULD_SUBMIT_METRICS", False):
                await self.submit_log_forwarder_metrics(log_forwarder_id, response.metrics, sub_id)
            self.log_forwarder_metric_cache[log_forwarder_id] = metric_dict
        except HttpResponseError as err:
            log.error(err)
        except RetryError:
            log.error("Max retries attempted")

    @retry(retry=retry_if_exception_type(ServiceResponseTimeoutError), stop=stop_after_attempt(MAX_ATTEMPS))
    async def get_log_forwarder_metrics(self, log_forwarder_id: str) -> MetricsQueryResult:
        return await self.client.query_resource(
            log_forwarder_id,
            metric_names=list(COLLECTED_METRIC_DEFINITIONS.keys()),
            timespan=timedelta(minutes=METRIC_COLLECTION_PERIOD),
            granularity=timedelta(minutes=METRIC_COLLECTION_GRANULARITY),
            timeout=CLIENT_MAX_SECONDS,
        )

    # @retry(stop=stop_after_attempt(MAX_ATTEMPS))
    async def submit_log_forwarder_metrics(self, log_forwarder_id: str, metrics: list[Metric], sub_id: str) -> None:
        if "DD_API_KEY" in os.environ:
            metric_series = await gather(*[self.create_metric_series(metric, log_forwarder_id) for metric in metrics])
            if not all(metric_series):
                log.warn(
                    f"Invalid timestamps for resource: {get_function_app_id(sub_id, get_config_option('RESOURCE_GROUP'), log_forwarder_id)}\nSkipping..."
                )
                return
            body = MetricPayload(
                series=metric_series,
            )
            configuration = Configuration()
            configuration.request_timeout = CLIENT_MAX_SECONDS
            async with AsyncApiClient(configuration) as api_client:
                api_instance = MetricsApi(api_client)
                response = await api_instance.submit_metrics(body=body)
                if len(response.get("errors", [])) > 0:
                    for err in response.get("errors", []):
                        log.error(err)
        else:
            log.warning("Metric API key is not set. Skipping submit metrics")
            return

    async def create_metric_series(self, metric: Metric, log_forwarder_id: str) -> MetricSeries | None:
        metric_points = await gather(
            *[
                self.create_metric_point(metric_value, COLLECTED_METRIC_DEFINITIONS.get(metric.name, ""))
                for time_series_element in metric.timeseries
                for metric_value in time_series_element.data
            ]
        )
        if not all(metric_points):
            return None
        return MetricSeries(
            metric=metric.name,
            type=MetricIntakeType.UNSPECIFIED,
            points=metric_points,
            resources=[
                MetricResource(
                    name=get_function_app_name(log_forwarder_id),
                    type="logforwarder",
                ),
            ],
        )

    async def create_metric_point(self, metric_value: MetricValue, metric_attr: str) -> MetricPoint | None:
        metric_timestamp = metric_value.timestamp.timestamp()
        if (datetime.now().timestamp() - metric_timestamp) > 3540:
            return None
        return MetricPoint(
            timestamp=int(metric_timestamp),
            value=getattr(metric_value, metric_attr, 0),
        )

    async def write_caches(self) -> None:
        log.info("Output_dict: " + str(self.log_forwarder_metric_cache))


async def main():
    basicConfig(level=INFO)
    log.info("Started task at %s", now())
    # This is holder code until assignment cache becomes availaible
    cache = dumps(
        {
            "0b62a232-b8db-4380-9da6-640f7272ed6d": {
                "east_us": {
                    "resources": {"diagnostic-settings-task": "d76404b14764"},
                    "configurations": {"d76404b14764": "storageaccount"},
                }
            },
        }
    )
    try:
        async with MonitorTask(cache) as task:
            await task.run()
    except InvalidCacheError:
        log.warning("Task skipped")
    log.info("Task finished at %s", now())


if __name__ == "__main__":
    asyncio.run(main())
