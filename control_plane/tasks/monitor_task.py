# stdlib
from asyncio import gather
import asyncio
from collections.abc import AsyncIterable
from copy import deepcopy
from datetime import datetime, timedelta
from json import dumps, loads
from logging import ERROR, INFO, basicConfig, getLogger
from typing import Any, Self, TypeAlias
from uuid import uuid4

# 3p
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.monitor.query.aio import MetricsQueryClient
from azure.monitor.query import MetricAggregationType
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.monitor.v2021_05_01_preview.models import (
    DiagnosticSettingsResource,
    LogSettings,
    CategoryType,
    Resource,
)

# project
from cache.common import read_cache, write_cache
from cache.diagnostic_settings_cache import (
    DIAGNOSTIC_SETTINGS_CACHE_BLOB,
    DiagnosticSettingConfiguration,
    deserialize_diagnostic_settings_cache,
)
from cache.resources_cache import RESOURCE_CACHE_BLOB, deserialize_resource_cache
from tasks.task import Task


# silence azure logging except for errors
getLogger("azure").setLevel(ERROR)

ResourceMetricCache: TypeAlias = dict[str, dict[str, int | float]]


DIAGNOSTIC_SETTINGS_TASK_NAME = "monitor_task"
EVENT_HUB_NAME_SETTING = "EVENT_HUB_NAME"
EVENT_HUB_NAMESPACE_SETTING = "EVENT_HUB_NAMESPACE"
DIAGNOSTIC_SETTING_PREFIX = "datadog_log_forwarding_"

log = getLogger(DIAGNOSTIC_SETTINGS_TASK_NAME)
log.setLevel(INFO)

class MonitorTask(Task):
    def __init__(self, assignment_cache_state: str) -> None:
        super().__init__()

        # read caches

        success, assignment_settings_cache = deserialize_diagnostic_settings_cache(assignment_cache_state)
        if not success:
            log.warning("Assignments Cache is in an invalid format, resetting the cache")
            assignment_settings_cache = {}

        self.assignment_settings_cache = loads(assignment_cache_state)
        self.resource_metric_cache = {}
        self.client = MetricsQueryClient(self.credential)

        # define metrics

        self.metric_defs = {"FunctionExecutionCount": "total"}

    async def __aenter__(self) -> Self:
        await super().__aenter__()
        await self.client.__aenter__()
        return self

    async def __aexit__(self, *_) -> None:
        await self.client.__aexit__()
        log.info("\n\n" + str(self.resource_metric_cache))
        await super().__aexit__()

    async def run(self, client: MetricsQueryClient) -> None:
        log.info("Crawling %s subscriptions", len(self.assignment_settings_cache))
        await gather(
            *[self.process_subscription(sub_id, resources, client) for sub_id, resources in self.assignment_settings_cache.items()]
        )

    async def process_subscription(self, sub_id: str, resources_per_region: dict[str, Any], client: MetricsQueryClient):
        for region_name,region_data in resources_per_region.items():
            count = 0
            for resource_name, resource_id in region_data["resources"].items():
                count += 1
                log.info(f"{resource_name=} {resource_id=} {count=}")
                metric_dict = {}
                try:
                    response = await client.query_resource(
                        resource_id,
                        metric_names=list(self.metric_defs.keys()),
                        timespan=timedelta(hours=2),
                        granularity=timedelta(minutes=15)
                    )

                    for metric in response.metrics:
                        log.info(metric.name)
                        log.info(metric.unit)
                        metric_vals = []
                        for time_series_element in metric.timeseries:
                            for metric_value in time_series_element.data:
                                log.info(metric_value.timestamp)
                                log.info(f"{metric.name}: {self.metric_defs[metric.name]} = {metric_value.__dict__[self.metric_defs[metric.name]]}")
                                metric_vals.append(metric_value.__dict__[self.metric_defs[metric.name]])
                        metric_dict[metric.name] = max(metric_vals[-1], metric_vals[-2])
                    self.resource_metric_cache[resource_id] = metric_dict


                except HttpResponseError as err:
                    log.error(err)

    async def write_caches(self) -> None:
       pass

def now() -> str:
    return datetime.now().isoformat()


async def main():
    basicConfig()
    log.info("Started task at %s", now())
    # This is holder code until assignment cache becomes availaible
    resources = dumps({
                "sub_id1": {
                    "EAST_US": {
                        "resources": {"diagnostic-settings-task": "subscriptions/0b62a232-b8db-4380-9da6-640f7272ed6d/resourceGroups/lfo/providers/Microsoft.Web/sites/resources-task"},
                        "configurations": {
                            "OLD_LOG_FORWARDER_ID": {
                                "type": "storageaccount",
                                "id": "OLD_LOG_FORWARDER_ID",
                                "storage_account_id": "some/storage/account",
                            },
                        },
                    }
                },
            })
    async with MonitorTask(resources) as task:
            await task.run(task.client)
    log.info("Task finished at %s", now())


if __name__ == "__main__":
    asyncio.run(main())
