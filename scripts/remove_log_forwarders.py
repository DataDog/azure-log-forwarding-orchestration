#!/usr/bin/env python
# script to remove all log forwarders

# stdlib
from asyncio import gather, run
from logging import INFO, WARNING, basicConfig, getLogger
from sys import argv

# 3p
# requires `pip install azure-mgmt-resource`
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.resource.resources.v2022_09_01.aio import ResourceManagementClient
from azure.mgmt.resource.resources.v2022_09_01.models import Resource
from tenacity import retry, stop_after_attempt

getLogger("azure").setLevel(WARNING)
log = getLogger("extension_cleanup")


FUNCTION_APP_PREFIX = "dd-blob-log-forwarder-"
ASP_PREFIX = "dd-log-forwarder-plan-"
STORAGE_ACCOUNT_PREFIX = "ddlogstorage"
DRY_RUN = False

BATCH_SIZE = 5


def parse_resource_group(resource_id: str) -> str:
    return resource_id.split("/")[4]


def should_delete(resource: Resource, resource_group: str | None) -> bool:
    if parse_resource_group(resource.id) != resource_group:  # type: ignore
        return False

    name: str = resource.name  # type: ignore
    match resource.type.lower():  # type: ignore
        case "microsoft.web/sites":
            return name.startswith(FUNCTION_APP_PREFIX)
        case "microsoft.web/serverfarms":
            return name.startswith(ASP_PREFIX)
        case "microsoft.storage/storageaccounts":
            return name.startswith(STORAGE_ACCOUNT_PREFIX)
        case _:
            return False


def partition_resources(
    resources: list[Resource],
) -> tuple[list[Resource], list[Resource]]:
    function_apps: list[Resource] = []
    everything_else: list[Resource] = []
    for resource in resources:
        if resource.type.lower() == "microsoft.web/sites":  # type: ignore
            function_apps.append(resource)
        else:
            everything_else.append(resource)
    return function_apps, everything_else


@retry(stop=stop_after_attempt(5))
async def delete_resource(client: ResourceManagementClient, resource: Resource):
    global DRY_RUN
    if DRY_RUN:
        log.info(f"Would delete {resource.id}")
        return
    log.info(f"Deleting... {resource.id}")
    future = await client.resources.begin_delete_by_id(
        resource.id,  # type: ignore
        api_version="2022-09-01",  # type: ignore
    )
    await future.result()
    log.info(f"Deleted {resource.id} ✅")


async def main():
    sub_id = "0b62a232-b8db-4380-9da6-640f7272ed6d"
    resource_group: str | None = "lfo"
    async with DefaultAzureCredential() as cred, ResourceManagementClient(
        cred, sub_id
    ) as client:
        resources = client.resources.list()
        function_apps, everything_else = partition_resources(
            [
                resource
                async for resource in resources
                if should_delete(resource, resource_group)
            ]
        )

        # delete any function apps before we delete the rest to avoid conflicts
        for i in range(0, len(function_apps), BATCH_SIZE):
            await gather(
                *[delete_resource(client, r) for r in function_apps[i : i + BATCH_SIZE]]
            )

        for i in range(0, len(everything_else), BATCH_SIZE):
            await gather(
                *[
                    delete_resource(client, r)
                    for r in everything_else[i : i + BATCH_SIZE]
                ]
            )


if __name__ == "__main__":
    basicConfig(level=INFO)
    DRY_RUN = "--dry-run" in argv
    if DRY_RUN:
        log.info("Dry run, no changes will be made")
    run(main())
