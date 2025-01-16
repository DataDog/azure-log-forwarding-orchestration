# stdlib
from collections.abc import AsyncGenerator, AsyncIterable, Callable, Mapping
from contextlib import AbstractAsyncContextManager
from logging import getLogger
from types import TracebackType
from typing import Final, Self, TypeAlias, cast

# 3p
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.resource.resources.v2021_01_01.aio import ResourceManagementClient
from azure.mgmt.resource.resources.v2021_01_01.models import GenericResourceExpanded

# project
from tasks.common import (
    CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX,
    DIAGNOSTIC_SETTINGS_TASK_PREFIX,
    FORWARDER_MANAGED_ENVIRONMENT_PREFIX,
    FORWARDER_STORAGE_ACCOUNT_PREFIX,
    RESOURCES_TASK_PREFIX,
    SCALING_TASK_PREFIX,
)
from tasks.concurrency import safe_collect
from tasks.constants import (
    ALLOWED_STORAGE_ACCOUNT_REGIONS,
    FETCHED_RESOURCE_TYPES,
    NESTED_VALID_RESOURCE_TYPES,
    NON_NESTED_VALID_RESOURCE_TYPES,
)

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


FetchSubResources: TypeAlias = Callable[[GenericResourceExpanded], AsyncIterable[str]]


async def get_storage_account_services(r: GenericResourceExpanded) -> AsyncGenerator[str]:
    for service_type in NESTED_VALID_RESOURCE_TYPES["microsoft.storage/storageaccounts"]:
        yield f"{r.id}/{service_type}/default"


async def make_sub_resource_extractor_for_name_and_rg() -> FetchSubResources: ...


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

        self._get_sub_resources_map: Final[Mapping[str, FetchSubResources | None]] = {
            "microsoft.azuredatatransfer/connections": None,  # {"flows"},
            "microsoft.cache/redisenterprise": None,  # {"databases"},
            "microsoft.cdn/profiles": None,  # {"endpoints"},
            "microsoft.healthcareapis/workspaces": None,  # {"dicomservices", "fhirservices", "iotconnectors"},
            "microsoft.machinelearningservices/workspaces": None,  # {"onlineendpoints"},
            "microsoft.media/mediaservices": None,  # {"liveevents", "streamingendpoints"},
            "microsoft.netapp/netappaccounts": None,  # { "capacitypools","capacitypools/volumes",  # this is also a doubly nested subtype but i dont wanna deal with that rn},
            "microsoft.network/networksecurityperimeters": None,  # {"profiles"},
            "microsoft.network/networkmanagers": None,  # {"ipampools"},
            "microsoft.notificationhubs/namespaces": None,  # {"notificationhubs"},
            "microsoft.powerbi/tenants": None,  # {"workspaces"},
            "microsoft.signalrservice/signalr": None,  # {"replicas"},
            "microsoft.signalrservice/webpubsub": None,  # {"replicas"},
            "microsoft.storage/storageaccounts": get_storage_account_services,  # {"blobservices", "fileservices", "queueservices", "tableservices"},
            "microsoft.sql/servers": None,  # {"databases"},
            "microsoft.sql/managedinstances": None,  # {"databases"},
            "microsoft.synapse/workspaces": None,  # {"bigdatapools", "kustopools", "scopepools", "sqlpools"},
            "microsoft.web/sites": None,  # {"slots"},
        }

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
            resources_per_region.setdefault(region, set()).update(
                await safe_collect(self.all_resource_ids_for_resource(r))
            )
            resource_count += 1
        log.debug("Subscription %s: Collected %s resources", self.subscription_id, resource_count)
        return resources_per_region

    async def all_resource_ids_for_resource(self, resource: GenericResourceExpanded) -> AsyncGenerator[str]:
        resource_id = cast(str, resource.id).lower()
        resource_type = cast(str, resource.type).lower()
        if resource_type in NON_NESTED_VALID_RESOURCE_TYPES:
            yield resource_id
        if get_sub_resources := self._get_sub_resources_map.get(resource_type):
            async for sub_resource in get_sub_resources(resource):
                yield sub_resource
