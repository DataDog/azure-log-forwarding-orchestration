# stdlib
from asyncio import gather
from collections.abc import AsyncGenerator, AsyncIterable, Callable, Iterable, Mapping
from contextlib import AbstractAsyncContextManager
from logging import getLogger
from types import TracebackType
from typing import Any, Final, Self, TypeAlias, cast

# 3p
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.core.tools import parse_resource_id
from azure.mgmt.resource.resources.v2021_01_01.aio import ResourceManagementClient
from azure.mgmt.resource.resources.v2021_01_01.models import GenericResourceExpanded
from azure.mgmt.sql.aio import SqlManagementClient
from azure.mgmt.web.v2023_12_01.aio import WebSiteManagementClient

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
        yield f"{r.id}/{service_type}/default".lower()


def safe_get_id(r: Any) -> str | None:
    if hasattr(r, "id") and isinstance(r.id, str):
        return r.id.lower()
    return None


def make_sub_resource_extractor_for_rg_and_name(f: Callable[[str, str], AsyncIterable[Any]]) -> FetchSubResources:
    """Creates an extractor for sub resource IDs based on the resource group and name"""

    async def _f(r: GenericResourceExpanded) -> AsyncGenerator[str]:
        resource_group = getattr(r, "resource_group", None)
        resource_name = r.name
        if not (resource_group and resource_name):  # fallback to parsing the resource id
            parsed = parse_resource_id(r.id)
            resource_group = parsed["resource_group"]
            resource_name = parsed["name"]
        log.debug("Extracting sub resources for %s", r.id)
        async for sub_resource in f(resource_group, resource_name):
            if hasattr(sub_resource, "value") and isinstance(sub_resource.value, Iterable):
                for resource in sub_resource.value:
                    if rid := safe_get_id(resource):
                        yield rid
            if rid := safe_get_id(sub_resource):
                yield rid

    return _f


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
        web_client = WebSiteManagementClient(cred, subscription_id)
        sql_client = SqlManagementClient(cred, subscription_id)
        # map of resource type to client and sub resource fetching function
        self._get_sub_resources_map: Final[
            Mapping[str, tuple[AbstractAsyncContextManager | None, FetchSubResources | None]]
        ] = {
            "microsoft.azuredatatransfer/connections": (None, None),  # {"flows"},
            "microsoft.cache/redisenterprise": (None, None),  # {"databases"},
            "microsoft.cdn/profiles": (None, None),  # {"endpoints"},
            "microsoft.healthcareapis/workspaces": (None, None),  # {"dicomservices", "fhirservices", "iotconnectors"},
            "microsoft.machinelearningservices/workspaces": (None, None),  # {"onlineendpoints"},
            "microsoft.media/mediaservices": (None, None),  # {"liveevents", "streamingendpoints"},
            "microsoft.netapp/netappaccounts": (
                None,
                None,
            ),  # { "capacitypools","capacitypools/volumes",  # this is also a doubly nested subtype but i dont wanna deal with that rn},
            "microsoft.network/networksecurityperimeters": (None, None),  # {"profiles"},
            "microsoft.network/networkmanagers": (None, None),  # {"ipampools"},
            "microsoft.notificationhubs/namespaces": (None, None),  # {"notificationhubs"},
            "microsoft.powerbi/tenants": (None, None),  # {"workspaces"},
            "microsoft.signalrservice/signalr": (None, None),  # {"replicas"},
            "microsoft.signalrservice/webpubsub": (None, None),  # {"replicas"},
            "microsoft.storage/storageaccounts": (None, get_storage_account_services),
            "microsoft.sql/servers": (
                sql_client,
                make_sub_resource_extractor_for_rg_and_name(sql_client.databases.list_by_server),
            ),
            "microsoft.sql/managedinstances": (
                None,
                make_sub_resource_extractor_for_rg_and_name(sql_client.managed_databases.list_by_instance),
            ),
            "microsoft.synapse/workspaces": (None, None),  # {"bigdatapools", "kustopools", "scopepools", "sqlpools"},
            "microsoft.web/sites": (
                web_client,
                make_sub_resource_extractor_for_rg_and_name(web_client.web_apps.list_slots),
            ),
        }

    async def __aenter__(self) -> Self:
        await gather(
            self.resources_client.__aenter__(),
            *(client.__aenter__() for client, _ in self._get_sub_resources_map.values() if client is not None),
        )
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        await gather(
            self.resources_client.__aexit__(exc_type, exc_val, exc_tb),
            *(
                client.__aexit__(exc_type, exc_val, exc_tb)
                for client, _ in self._get_sub_resources_map.values()
                if client is not None
            ),
        )

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
        if resource_type in self._get_sub_resources_map and (
            get_sub_resources := self._get_sub_resources_map[resource_type][1]
        ):
            async for sub_resource in get_sub_resources(resource):
                yield sub_resource
