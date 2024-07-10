#!/usr/bin/env python3.12
# script to remove all log forwarders

# stdlib
from asyncio import gather, run
from logging import INFO, basicConfig, getLogger, WARNING
from sys import argv

# 3p

# requires `pip install azure-mgmt-resource`
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.resource.resources.v2022_09_01.aio import ResourceManagementClient
from azure.mgmt.resource.resources.v2022_09_01.models import Resource


getLogger("azure").setLevel(WARNING)
log = getLogger("extension_cleanup")


FUNCTION_APP_PREFIX = "dd-blob-log-forwarder-"
ASP_PREFIX = "dd-log-forwarder-plan-"
STORAGE_ACCOUNT_PREFIX = "ddlogstorage"
DRY_RUN = False


def should_delete(resource: Resource) -> bool:
    name: str = resource.name
    match resource.type.lower():
        case "microsoft.web/sites":
            return name.startswith(FUNCTION_APP_PREFIX)
        case "microsoft.web/serverfarms":
            return name.startswith(ASP_PREFIX)
        case "microsoft.storage/storageaccounts":
            return name.startswith(STORAGE_ACCOUNT_PREFIX)
        case _:
            return False


async def delete_resource(client: ResourceManagementClient, resource: Resource):
    global DRY_RUN
    if DRY_RUN:
        log.info(f"Would delete {resource.id}")
        return
    log.info(f"Deleting... {resource.id}")
    future = await client.resources.begin_delete_by_id(
        resource.id, api_version="2022-09-01"
    )
    await future.result()
    log.info(f"Deleted {resource.id} ✅")


async def main():
    sub_id = "0b62a232-b8db-4380-9da6-640f7272ed6d"
    async with DefaultAzureCredential() as cred, ResourceManagementClient(
        cred, sub_id
    ) as client:
        resources = client.resources.list()
        resources_to_delete = [
            resource async for resource in resources if should_delete(resource)
        ]
        await gather(
            *[delete_resource(client, resource) for resource in resources_to_delete]
        )


if __name__ == "__main__":
    basicConfig(level=INFO)
    DRY_RUN = "--dry-run" in argv
    if DRY_RUN:
        print("Dry run, no changes will be made")
    run(main())
