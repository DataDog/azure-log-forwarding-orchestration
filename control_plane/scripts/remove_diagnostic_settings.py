#!/usr/bin/env python3
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

"""
Script which removes all diagnostic settings in a given subscription.
Enumerates all resources in each subscription and removes all diagnostic settings
with the datadog_log_forwarding_ prefix.
"""

# stdlib
import argparse
from asyncio import Semaphore, gather, run
from collections.abc import AsyncIterable
from logging import ERROR, basicConfig
from typing import TypeVar

# 3p
from azure.core.exceptions import ResourceNotFoundError
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.monitor.v2021_05_01_preview.aio import MonitorManagementClient
from azure.mgmt.resource.resources.aio import ResourceManagementClient
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential_jitter

# project
from tasks.constants import FETCHED_RESOURCE_TYPES

DIAGNOSTIC_SETTING_PREFIX = "datadog_log_forwarding_"

MAX_CONCURRENCY = 100

RESOURCE_TYPE_FILTER = " or ".join(f"resourceType eq '{rt}'" for rt in FETCHED_RESOURCE_TYPES)


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


async def get_resources(cred: DefaultAzureCredential, subscription_id: str) -> set[str]:
    async with ResourceManagementClient(cred, subscription_id) as client:
        return {resource.id async for resource in client.resources.list(filter=RESOURCE_TYPE_FILTER) if resource.id}


async def list_diagnostic_settings(client: MonitorManagementClient, resource: str, s: Semaphore) -> list[str]:
    try:
        return await collect(
            (
                str(ds.name)
                async for ds in client.diagnostic_settings.list(resource)
                if str(ds.name).startswith(DIAGNOSTIC_SETTING_PREFIX)
            ),
            s,
        )
    except ResourceNotFoundError:
        return []


async def process_subscription(cred: DefaultAzureCredential, subscription_id: str, resources: set[str]) -> None:
    async with MonitorManagementClient(cred, subscription_id) as client:
        s = Semaphore(MAX_CONCURRENCY)
        diagnostic_settings = await gather(
            *(list_diagnostic_settings(client, resource, s) for resource in resources),
        )
        errors = await gather(
            *(delete_diagnostic_settings(client, rid, ds, s) for rid, ds in zip(resources, diagnostic_settings)),
            return_exceptions=True,
        )
        if any(errors):
            print("Errors processing subscription", subscription_id, [error for error in errors if error])
        else:
            print("Successfully processed subscription", subscription_id)


async def main(subscriptions: list[str]) -> None:
    basicConfig(level=ERROR)
    async with DefaultAzureCredential() as cred:
        print("Fetching resources from Azure")
        resource_sets = await gather(*(get_resources(cred, sub) for sub in subscriptions))
        total = sum(len(r) for r in resource_sets)
        print(f"Found {total} resources across {len(subscriptions)} subscriptions")
        await gather(
            *(process_subscription(cred, sub, resources) for sub, resources in zip(subscriptions, resource_sets))
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remove Datadog diagnostic settings from Azure subscriptions")
    parser.add_argument(
        "subscriptions", nargs="+", metavar="SUBSCRIPTION_ID", help="One or more Azure subscription IDs"
    )
    args = parser.parse_args()
    run(main(args.subscriptions))
