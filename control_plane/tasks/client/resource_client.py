# stdlib
from contextlib import AbstractAsyncContextManager
from logging import getLogger
from types import TracebackType
from typing import Final, Self, cast

# 3p
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.resource.resources.v2021_01_01.aio import ResourceManagementClient

# project
from tasks.common import (
    CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX,
    DIAGNOSTIC_SETTINGS_TASK_PREFIX,
    FORWARDER_MANAGED_ENVIRONMENT_PREFIX,
    FORWARDER_STORAGE_ACCOUNT_PREFIX,
    RESOURCES_TASK_PREFIX,
    SCALING_TASK_PREFIX,
)
from tasks.constants import ALLOWED_STORAGE_ACCOUNT_REGIONS, FETCHED_RESOURCE_TYPES

log = getLogger(__name__)

RESOURCE_QUERY_FILTER: Final = " or ".join(f"resourceType eq '{rt}'" for rt in FETCHED_RESOURCE_TYPES)

# we only need to ignore forwardable resource types here, since others will be filtered by type.
IGNORED_LFO_PREFIXES: Final = frozenset(
    {
        FORWARDER_MANAGED_ENVIRONMENT_PREFIX,
        SCALING_TASK_PREFIX,
        RESOURCES_TASK_PREFIX,
        DIAGNOSTIC_SETTINGS_TASK_PREFIX,
        CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX,
        FORWARDER_STORAGE_ACCOUNT_PREFIX,
    }
)


def should_ignore_resource(region: str, resource_type: str, resource_name: str) -> bool:
    """Determines if we should ignore the resource"""
    name = resource_name.lower()
    return (
        # we must be able to put a storage account in the same region
        # https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/diagnostic-settings#destination-limitations
        region.lower() not in ALLOWED_STORAGE_ACCOUNT_REGIONS
        # ignore resources that are managed by the control plane
        or any(name.startswith(prefix) for prefix in IGNORED_LFO_PREFIXES)
        # only certain resource types have diagnostic settings, this is a confirmation that the filter worked
        or resource_type.lower() not in FETCHED_RESOURCE_TYPES
    )


class ResourceClient(AbstractAsyncContextManager["ResourceClient"]):
    def __init__(self, cred: DefaultAzureCredential, subscription_id: str) -> None:
        super().__init__()
        self.credential = cred
        self.subscription_id = subscription_id
        self.resources_client = ResourceManagementClient(cred, subscription_id)

    async def __aenter__(self) -> Self:
        await self.resources_client.__aenter__()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        await self.resources_client.__aexit__(exc_type, exc_val, exc_tb)

    async def get_resources_per_region(self) -> dict[str, set[str]]:
        resources_per_region: dict[str, set[str]] = {}
        resource_count = 0
        async for r in self.resources_client.resources.list(RESOURCE_QUERY_FILTER):
            region = cast(str, r.location).lower()
            if should_ignore_resource(region, cast(str, r.type), cast(str, r.name)):
                continue
            resources_per_region.setdefault(region, set()).add(cast(str, r.id).lower())
            resource_count += 1
        log.debug("Subscription %s: Collected %s resources", self.subscription_id, resource_count)
        return resources_per_region
