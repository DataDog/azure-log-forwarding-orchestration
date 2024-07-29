#!/usr/bin/env python3.11
# script to remove diagnostic settings from all resources

# stdlib
from asyncio import gather, run, create_task
from itertools import chain
from logging import INFO, basicConfig, getLogger, WARNING
from sys import argv
from typing import Any, AsyncIterable, Callable, Coroutine, Iterable, TypeVar

# 3p

# requires `pip install azure-mgmt-resource`
from azure.identity.aio import DefaultAzureCredential
from azure.core.credentials_async import AsyncTokenCredential
from azure.mgmt.resource.resources.aio import ResourceManagementClient
from azure.mgmt.monitor.v2021_05_01_preview.aio import MonitorManagementClient


getLogger("azure").setLevel(WARNING)
log = getLogger("extension_cleanup")
T = TypeVar("T")
R = TypeVar("R")

# resource_idx = 0

DIAGNOSTIC_SETTING_PREFIX = "datadog_log_forwarding_"
DRY_RUN = False


def flatten(items: Iterable[Any]) -> Iterable[Any]:
    """Yield items from any nested iterable; see REF."""
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, str | bytes):
            yield from flatten(x)
        else:
            yield x


async def run_parallel(
    async_func: Callable[[T], Coroutine[Any, Any, R]], async_iter: AsyncIterable[T]
) -> list[R | BaseException]:
    return await gather(
        *[create_task(async_func(item)) async for item in async_iter],
        return_exceptions=True,
    )


async def delete_setting(
    client: MonitorManagementClient, resource_id: str, setting: str
):
    if setting and setting.startswith(DIAGNOSTIC_SETTING_PREFIX):
        try:
            if not DRY_RUN:
                await client.diagnostic_settings.delete(resource_id, setting)
            log.info(f"Deleted setting {setting} for resource {resource_id} ✅")
        except Exception:
            log.exception(
                f"Failed to delete setting {setting} for resource {resource_id}"
            )


async def process_resource(
    monitor_client: MonitorManagementClient, resource_id: str
) -> list[BaseException]:
    res = await run_parallel(
        lambda setting: delete_setting(monitor_client, resource_id, setting.name),  # type: ignore
        monitor_client.diagnostic_settings.list(resource_id),
    )
    return list(filter(None, res))


async def process_subscription(
    cred: AsyncTokenCredential, sub_id: str
) -> list[BaseException]:
    async with (
        ResourceManagementClient(cred, sub_id) as resource_client,
        MonitorManagementClient(cred, sub_id) as monitor_client,
    ):
        log.info(f"Processing subscription {sub_id}")
        res = await run_parallel(
            lambda resource: process_resource(monitor_client, resource.id),  # type: ignore
            resource_client.resources.list(),
        )
        return list(flatten(res))


async def main():
    subs = ["0b62a232-b8db-4380-9da6-640f7272ed6d"]
    if subs != ["0b62a232-b8db-4380-9da6-640f7272ed6d"]:
        log.error("wrong subs, something broke: %s", subs)
        return
    async with DefaultAzureCredential() as cred:
        res = await gather(*[process_subscription(cred, sub) for sub in subs])
    for err in chain(*res):
        if "ResourceTypeNotSupported" not in str(err):
            log.exception(
                "Unexpected exception in deleting diagnostic settings", exc_info=err
            )


if __name__ == "__main__":
    basicConfig(level=INFO)
    DRY_RUN = "--dry-run" in argv
    if DRY_RUN:
        log.info("Dry run, no changes will be made")
    run(main())
