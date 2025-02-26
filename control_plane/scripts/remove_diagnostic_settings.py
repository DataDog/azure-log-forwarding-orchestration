#!/usr/bin/env python3
"""
Script which removes all diagnostic settings in a given subscription.
Runs the resources task to get all resources in the subscription
and then removes all diagnostic settings.
"""

# stdlib
import json
import os
from asyncio import Semaphore, gather, run
from collections.abc import AsyncIterable
from contextlib import suppress
from itertools import chain
from logging import ERROR, basicConfig
from typing import TypeVar

# 3p
from azure.core.exceptions import ResourceNotFoundError
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.monitor.v2021_05_01_preview.aio import MonitorManagementClient
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential_jitter

from cache.resources_cache import ResourceCache

# project
from tasks.resources_task import ResourcesTask

LOG_FORWARDING_TESTING = "34464906-34fe-401e-a420-79bd0ce2a1da"
AZURE_INTS_TESTING = "0b62a232-b8db-4380-9da6-640f7272ed6d"
DIAGNOSTIC_SETTING_PREFIX = "datadog_log_forwarding_"

MONITORED_SUBSCRIPTIONS = [LOG_FORWARDING_TESTING, AZURE_INTS_TESTING]
os.environ["MONITORED_SUBSCRIPTIONS_SETTING"] = json.dumps(MONITORED_SUBSCRIPTIONS)

MAX_CONCURRENCY = 100


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(),
    retry=retry_if_exception(lambda e: not isinstance(e, ResourceNotFoundError)),
)
async def delete_diagnostic_settings(
    client: MonitorManagementClient, resource_id: str, ds_names: list[str], s: Semaphore
) -> None:
    async with s:
        await gather(*(client.diagnostic_settings.delete(resource_id, ds) for ds in ds_names))


T = TypeVar("T")


async def collect(it: AsyncIterable[T], s: Semaphore) -> list[T]:
    async with s:
        return [item async for item in it]


async def process_subscription(cred: DefaultAzureCredential, subscription_id: str, resources: set[str]) -> None:
    async with MonitorManagementClient(cred, subscription_id) as client:
        s = Semaphore(MAX_CONCURRENCY)
        diagnostic_settings = await gather(
            *(
                collect(
                    (
                        str(s.name)
                        async for s in client.diagnostic_settings.list(resource)
                        if str(s.name).startswith(DIAGNOSTIC_SETTING_PREFIX)
                    ),
                    s,
                )
                for resource in resources
            ),
        )
        errors = await gather(
            *(delete_diagnostic_settings(client, rid, ds, s) for rid, ds in zip(resources, diagnostic_settings)),
            return_exceptions=True,
        )
        if any(errors):
            print("Errors processing subscription", subscription_id, [error for error in errors if error])
        else:
            print("Successfully processed subscription", subscription_id)


async def main():
    print("Running Resources Task to get all resources")
    basicConfig(level=ERROR)
    cache: ResourceCache = {}
    with suppress(Exception):
        async with ResourcesTask("") as resources_task:
            await resources_task.run()
            cache = resources_task.resource_cache
    print(
        f"Resources Task completed, found {sum(len(resources) for regions in cache.values() for resources in regions.values())} resources"
    )
    async with DefaultAzureCredential() as cred:
        await gather(
            *(process_subscription(cred, sub, set(chain(*cache[sub].values()))) for sub in MONITORED_SUBSCRIPTIONS)
        )


if __name__ == "__main__":
    run(main())
