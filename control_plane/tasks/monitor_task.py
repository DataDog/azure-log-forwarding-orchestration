# stdlib
from asyncio import gather
import asyncio
from collections.abc import AsyncIterable
from copy import deepcopy
from datetime import datetime, timedelta
from json import dumps
from logging import ERROR, INFO, basicConfig, getLogger
from typing import Any
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

        self.assignment_settings_cache = assignment_settings_cache

    async def run(self, client: MetricsQueryClient) -> None:
        self.assignment_settings_cache = {
                "sub_id1": {
                    "EAST_US": {
                        "resources": {"diagnostic-settings-task": "/subscriptions/0b62a232-b8db-4380-9da6-640f7272ed6d/resourceGroups/lfo/providers/Microsoft.Web/sites/diagnostic-settings-task", "diagnostic-settings-task2": "/subscriptions/0b62a232-b8db-4380-9da6-640f7272ed6d/resourceGroups/lfo/providers/Microsoft.Web/sites/diagnostic-settings-task"},
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
        log.info("Crawling %s subscriptions", len(self.assignment_settings_cache))
        await gather(
            *[self.process_subscription(sub_id, resources, client) for sub_id, resources in self.assignment_settings_cache.items()]
        )

    async def process_subscription(self, sub_id: str, resources_per_region: dict[str, Any], client: MetricsQueryClient):
        for k,v in resources_per_region.items():
            count = 0
            for resource_name, resource_id in v["resources"].items():
                count = count + 1
                log.info(resource_name)
                log.info(resource_id)
                log.info(count)
                try:
                    response = await client.query_resource(
                        resource_id,
                        metric_names=["HttpResponseTime"],
                        timespan=timedelta(hours=2),
                        granularity=timedelta(minutes=15),
                        aggregations=[MetricAggregationType.AVERAGE]
                    )

                    for metric in response.metrics:
                        log.info(metric.name)
                        log.info(metric.unit)
                        for time_series_element in metric.timeseries:
                            for metric_value in time_series_element.data:
                                log.info(metric_value.timestamp)
                                log.info(metric_value.average)
                except HttpResponseError as err:
                    log.error(err)
        return

    async def write_caches(self) -> None:
       pass

def now() -> str:
    return datetime.now().isoformat()


async def main():
    basicConfig()
    log.info("Started task at %s", now())
    # This is holder code until assignment cache becomes availaible
    resources = ""
    async with DefaultAzureCredential() as credential, MonitorTask(resources) as task, MetricsQueryClient(credential) as client:
            await task.run(client)
    log.info("Task finished at %s", now())


if __name__ == "__main__":
    asyncio.run(main())
