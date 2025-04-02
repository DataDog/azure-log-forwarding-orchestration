# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/  Copyright 2025 Datadog, Inc.

# stdlib
from asyncio import gather
from collections.abc import AsyncGenerator, AsyncIterable, Callable, Iterable, Mapping
from contextlib import AbstractAsyncContextManager
from itertools import chain
from logging import Logger
from types import TracebackType
from typing import Any, Final, Protocol, Self, TypeAlias, cast

# 3p
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.cdn.aio import CdnManagementClient
from azure.mgmt.core.tools import parse_resource_id
from azure.mgmt.healthcareapis.aio import HealthcareApisManagementClient
from azure.mgmt.media.aio import AzureMediaServices
from azure.mgmt.netapp.aio import NetAppManagementClient
from azure.mgmt.network.aio import NetworkManagementClient
from azure.mgmt.notificationhubs.aio import NotificationHubsManagementClient
from azure.mgmt.powerbiembedded.aio import PowerBIEmbeddedManagementClient
from azure.mgmt.redisenterprise.aio import RedisEnterpriseManagementClient
from azure.mgmt.resource.resources.v2021_01_01.aio import ResourceManagementClient
from azure.mgmt.resource.resources.v2021_01_01.models import GenericResourceExpanded
from azure.mgmt.sql.aio import SqlManagementClient
from azure.mgmt.synapse.aio import SynapseManagementClient
from azure.mgmt.web.v2024_04_01.aio import WebSiteManagementClient

# project
from cache.resources_cache import RegionToResourcesDict, ResourceMetadata
from tasks.client.filtering import parse_filtering_rule
from tasks.common import (
    CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX,
    DIAGNOSTIC_SETTINGS_TASK_PREFIX,
    FORWARDER_MANAGED_ENVIRONMENT_PREFIX,
    FORWARDER_STORAGE_ACCOUNT_PREFIX,
    RESOURCES_TASK_PREFIX,
    SCALING_TASK_PREFIX,
    resource_tag_dict_to_list,
)
from tasks.concurrency import safe_collect
from tasks.constants import (
    ALLOWED_STORAGE_ACCOUNT_REGIONS,
    FETCHED_RESOURCE_TYPES,
    NESTED_VALID_RESOURCE_TYPES,
    UNNESTED_VALID_RESOURCE_TYPES,
)

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


class SDKClientMethod(Protocol):
    def __call__(self, resource_group: str, resource_name: str, /, **kwargs: Any) -> AsyncIterable[Any]: ...


async def get_storage_account_services(r: GenericResourceExpanded) -> AsyncGenerator[str]:
    for service_type in NESTED_VALID_RESOURCE_TYPES["microsoft.storage/storageaccounts"]:
        yield f"{r.id}/{service_type}/default".lower()


def safe_get_id(r: Any) -> str | None:
    if hasattr(r, "id") and isinstance(r.id, str):
        return r.id.lower()
    return None


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
    def __init__(self, log: Logger, cred: DefaultAzureCredential, tag_filters: list[str], subscription_id: str) -> None:
        super().__init__()
        self.log = log
        self.credential = cred
        self.subscription_id = subscription_id
        self.should_include = parse_filtering_rule(tag_filters)
        """Predicate which takes a resource's tags as input and evaluates them against the
        user-configured `tag_filters` to determine whether the resource's logs should be forwarded"""

        self.resources_client = ResourceManagementClient(cred, subscription_id)
        redis_client = RedisEnterpriseManagementClient(cred, subscription_id)
        cdn_client = CdnManagementClient(cred, subscription_id)
        healthcareapis_client = HealthcareApisManagementClient(cred, subscription_id)
        media_client = AzureMediaServices(cred, subscription_id)
        network_client = NetworkManagementClient(cred, subscription_id)
        netapp_client = NetAppManagementClient(cred, subscription_id)
        notificationhubs_client = NotificationHubsManagementClient(cred, subscription_id)
        powerbi_client = PowerBIEmbeddedManagementClient(cred, subscription_id)
        sql_client = SqlManagementClient(cred, subscription_id)
        synapse_client = SynapseManagementClient(cred, subscription_id)
        web_client = WebSiteManagementClient(cred, subscription_id)

        # map of resource type to client and sub resource fetching function
        self._get_sub_resources_map: Final[
            Mapping[str, tuple[AbstractAsyncContextManager | None, FetchSubResources]]
        ] = {
            "microsoft.cache/redisenterprise": (
                redis_client,
                self.make_sub_resource_extractor_for_rg_and_name(redis_client.databases.list_by_cluster),
            ),
            "microsoft.cdn/profiles": (
                cdn_client,
                self.make_sub_resource_extractor_for_rg_and_name(cdn_client.endpoints.list_by_profile),
            ),
            "microsoft.healthcareapis/workspaces": (
                healthcareapis_client,
                self.make_sub_resource_extractor_for_rg_and_name(
                    healthcareapis_client.dicom_services.list_by_workspace,
                    healthcareapis_client.fhir_services.list_by_workspace,
                    healthcareapis_client.iot_connectors.list_by_workspace,
                ),
            ),
            "microsoft.media/mediaservices": (
                media_client,
                self.make_sub_resource_extractor_for_rg_and_name(
                    media_client.live_events.list, media_client.streaming_endpoints.list
                ),
            ),
            "microsoft.netapp/netappaccounts": (
                netapp_client,
                self.make_sub_resource_extractor_for_rg_and_name(netapp_client.pools.list),
            ),
            "microsoft.network/networkmanagers": (
                network_client,
                self.make_sub_resource_extractor_for_rg_and_name(network_client.ipam_pools.list),
            ),
            "microsoft.notificationhubs/namespaces": (
                notificationhubs_client,
                self.make_sub_resource_extractor_for_rg_and_name(notificationhubs_client.notification_hubs.list),
            ),
            "microsoft.powerbi/tenants": (
                powerbi_client,
                self.make_sub_resource_extractor_for_rg_and_name(powerbi_client.workspaces.list),
            ),
            "microsoft.storage/storageaccounts": (None, get_storage_account_services),
            "microsoft.sql/servers": (
                sql_client,
                self.make_sub_resource_extractor_for_rg_and_name(sql_client.databases.list_by_server),
            ),
            "microsoft.sql/managedinstances": (
                None,
                self.make_sub_resource_extractor_for_rg_and_name(sql_client.managed_databases.list_by_instance),
            ),
            "microsoft.synapse/workspaces": (
                synapse_client,
                self.make_sub_resource_extractor_for_rg_and_name(
                    synapse_client.big_data_pools.list_by_workspace,
                    synapse_client.sql_pools.list_by_workspace,
                ),
            ),
            "microsoft.web/sites": (
                web_client,
                self.make_sub_resource_extractor_for_rg_and_name(web_client.web_apps.list_slots),
            ),
        }

    def make_sub_resource_extractor_for_rg_and_name(self, *functions: SDKClientMethod) -> FetchSubResources:
        """Creates an extractor for sub resource IDs based on the resource group and name"""

        async def _f(r: GenericResourceExpanded) -> AsyncGenerator[str]:
            resource_group = getattr(r, "resource_group", None)
            resource_name = r.name
            if not isinstance(resource_group, str) or not isinstance(
                resource_name, str
            ):  # fallback to parsing the resource id
                parsed = parse_resource_id(cast(str, r.id))
                resource_group = cast(str, parsed["resource_group"])
                resource_name = cast(str, parsed["name"])
            self.log.debug("Extracting sub resources for %s", r.id)
            sub_resources = await gather(
                *(safe_collect(f(resource_group, resource_name, timeout=30), self.log) for f in functions)
            )
            for sub_resource in chain.from_iterable(sub_resources):
                if hasattr(sub_resource, "value") and isinstance(sub_resource.value, Iterable):
                    for resource in sub_resource.value:
                        if rid := safe_get_id(resource):
                            yield rid
                elif rid := safe_get_id(sub_resource):
                    yield rid

        return _f

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

    async def get_resources_per_region(self) -> RegionToResourcesDict:
        resources_per_region: RegionToResourcesDict = {}

        resources = await safe_collect(self.resources_client.resources.list(RESOURCE_QUERY_FILTER), self.log)
        valid_resources = [
            r
            for r in resources
            if not should_ignore_resource(cast(str, r.location), cast(str, r.type), cast(str, r.name))
        ]
        self.log.debug(
            "Collected %s valid resources for subscription %s, fetching sub-resources...",
            len(valid_resources),
            self.subscription_id,
        )
        batched_resource_ids = await gather(
            *(safe_collect(self.all_resource_ids_for_resource(r), self.log) for r in valid_resources)
        )
        for resource, resource_ids in zip(valid_resources, batched_resource_ids, strict=False):
            region = cast(str, resource.location).lower()
            resource_tags = resource_tag_dict_to_list(resource.tags)
            metadata = ResourceMetadata(include=self.should_include(resource_tags))
            resources_per_region.setdefault(region, {}).update({id: metadata for id in resource_ids})

        self.log.info(
            "Subscription %s: Collected %s resources",
            self.subscription_id,
            sum(len(rs) for rs in resources_per_region.values()),
        )
        return resources_per_region

    async def all_resource_ids_for_resource(self, resource: GenericResourceExpanded) -> AsyncGenerator[str]:
        resource_id = cast(str, resource.id).lower()
        resource_type = cast(str, resource.type).lower()
        if resource_type in UNNESTED_VALID_RESOURCE_TYPES:
            yield resource_id
        if resource_type in self._get_sub_resources_map:
            _, get_sub_resources = self._get_sub_resources_map[resource_type]
            async for sub_resource in get_sub_resources(resource):
                yield sub_resource
