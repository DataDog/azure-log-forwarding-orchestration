# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from collections.abc import Iterable, Mapping
from datetime import datetime
from logging import Logger, getLogger
from math import inf
from os import environ
from typing import Final, Protocol, TypeVar
from uuid import uuid4

# 3p
from azure.identity.aio import DefaultAzureCredential

# project
<<<<<<< Updated upstream
from cache.env import CONTROL_PLANE_REGION_SETTING, get_config_option
=======
from cache.common import read_cache, write_cache
from cache.env import CONTROL_PLANE_REGION_SETTING

log = getLogger(__name__)
>>>>>>> Stashed changes

LFO_METRIC_PREFIX = "azure.lfo."
CONTROL_PLANE_METRIC_PREFIX = LFO_METRIC_PREFIX + "control_plane."
FORWARDER_METRIC_PREFIX = LFO_METRIC_PREFIX + "forwarder."

CONTROL_PLANE_APP_SERVICE_PLAN_PREFIX: Final = "dd-lfo-control-"
CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX: Final = "ddlfocontrol"
SCALING_TASK_PREFIX: Final = "scaling-task-"
RESOURCES_TASK_PREFIX: Final = "resources-task-"
DIAGNOSTIC_SETTINGS_TASK_PREFIX: Final = "diagnostic-settings-task-"


FORWARDER_CONTAINER_APP_PREFIX: Final = "dd-log-forwarder-"
FORWARDER_MANAGED_ENVIRONMENT_PREFIX: Final = "dd-log-forwarder-env-"
FORWARDER_STORAGE_ACCOUNT_PREFIX: Final = "ddlogstorage"


# TODO We will need to add prefixes for these when we implement event hub support
EVENT_HUB_NAME_PREFIX: Final = NotImplemented
EVENT_HUB_NAMESPACE_PREFIX: Final = NotImplemented


def get_container_app_name(config_id: str) -> str:
    return FORWARDER_CONTAINER_APP_PREFIX + config_id


def get_resource_group_id(subscription_id: str, resource_group: str) -> str:
    return f"/subscriptions/{subscription_id}/resourcegroups/{resource_group}".lower()


def get_container_app_id(subscription_id: str, resource_group: str, config_id: str) -> str:
    return (
        get_resource_group_id(subscription_id, resource_group)
        + "/providers/microsoft.app/jobs/"
        + get_container_app_name(config_id)
    ).lower()


def get_managed_env_name(region: str, control_plane_id: str) -> str:
    return f"{FORWARDER_MANAGED_ENVIRONMENT_PREFIX}{control_plane_id}-{region}"


def get_managed_env_id(subscription_id: str, resource_group: str, region: str, control_plane_id: str) -> str:
    return (
        get_resource_group_id(subscription_id, resource_group)
        + "/providers/microsoft.app/managedenvironments/"
        + get_managed_env_name(region, control_plane_id)
    ).lower()


def get_storage_account_name(config_id: str) -> str:
    return FORWARDER_STORAGE_ACCOUNT_PREFIX + config_id


def get_storage_account_id(subscription_id: str, resource_group: str, config_id: str) -> str:
    return (
        get_resource_group_id(subscription_id, resource_group)
        + "/providers/microsoft.storage/storageaccounts/"
        + get_storage_account_name(config_id)
    ).lower()


# https://learn.microsoft.com/en-us/azure/azure-government/compare-azure-government-global-azure
def is_azure_gov(region: str) -> bool:
    return region.lower().startswith("usgov")


def is_azure_china(region: str) -> bool:
    return region.lower().startswith("china")


<<<<<<< Updated upstream
def get_authority_for_region(region: str) -> str | None:
=======
# Authority endpoints for different Azure clouds
AZURE_PUBLIC_AUTHORITY: Final = "login.microsoftonline.com"
AZURE_GOV_AUTHORITY: Final = "login.microsoftonline.us"
AZURE_CHINA_AUTHORITY: Final = "login.chinacloudapi.cn"

# Cache blob name for storing detected authority
AUTHORITY_CACHE_BLOB: Final = "authority.txt"

# Order to probe clouds when detecting authority
AUTHORITIES_TO_PROBE: Final = (AZURE_PUBLIC_AUTHORITY, AZURE_GOV_AUTHORITY, AZURE_CHINA_AUTHORITY)


def get_authority_for_region(region: str) -> str:
>>>>>>> Stashed changes
    """Return the appropriate Azure authority based on the region.

    - Azure Government (usgov*) -> login.microsoftonline.us
    - Azure China (china*) -> login.chinacloudapi.cn
<<<<<<< Updated upstream
    - Azure Public (all others) -> None (use default)
    """
    if is_azure_gov(region):
        return "login.microsoftonline.us"
    if is_azure_china(region):
        return "login.chinacloudapi.cn"
    return None


def create_credential() -> DefaultAzureCredential:
    """Create a DefaultAzureCredential with the appropriate authority for the current region."""
    region = get_config_option(CONTROL_PLANE_REGION_SETTING)
    authority = get_authority_for_region(region)
    if authority:
        return DefaultAzureCredential(authority=authority)
    return DefaultAzureCredential()
=======
    - Azure Public (all others) -> login.microsoftonline.com
    """
    if is_azure_gov(region):
        return AZURE_GOV_AUTHORITY
    if is_azure_china(region):
        return AZURE_CHINA_AUTHORITY
    return AZURE_PUBLIC_AUTHORITY


def create_credential() -> DefaultAzureCredential:
    """Create a DefaultAzureCredential with the appropriate authority for the current environment.

    Uses CONTROL_PLANE_REGION if set to determine the authority, otherwise defaults to public cloud.
    For auto-detection when region is not set, use create_credential_with_probing() instead.
    """
    region = environ.get(CONTROL_PLANE_REGION_SETTING)
    authority = get_authority_for_region(region) if region else AZURE_PUBLIC_AUTHORITY
    return DefaultAzureCredential(authority=authority)


async def _probe_authority(authority: str) -> bool:
    """Try to authenticate with the given authority and check if we can list subscriptions.

    Returns True if at least one subscription is found, False otherwise.
    """
    # Import here to avoid import errors when the package isn't installed (e.g., in tests)
    from azure.mgmt.subscription.aio import SubscriptionClient

    try:
        credential = DefaultAzureCredential(authority=authority)
        async with credential:
            async with SubscriptionClient(credential) as sub_client:
                async for _ in sub_client.subscriptions.list():
                    # Found at least one subscription - this cloud works
                    log.info("Successfully detected Azure cloud with authority: %s", authority)
                    return True
        log.debug("No subscriptions found with authority: %s", authority)
        return False
    except Exception as e:
        log.debug("Failed to authenticate with authority %s: %s", authority, e)
        return False


async def _detect_and_cache_authority() -> str:
    """Probe each Azure cloud until we find one with subscriptions, then cache the result.

    Returns the detected authority.
    """
    for authority in AUTHORITIES_TO_PROBE:
        if await _probe_authority(authority):
            await write_cache(AUTHORITY_CACHE_BLOB, authority)
            return authority

    # Fallback to public cloud if nothing worked
    log.warning("Could not detect Azure cloud, falling back to public cloud")
    await write_cache(AUTHORITY_CACHE_BLOB, AZURE_PUBLIC_AUTHORITY)
    return AZURE_PUBLIC_AUTHORITY


async def create_credential_with_probing() -> DefaultAzureCredential:
    """Create a DefaultAzureCredential, probing for the correct cloud if region is not set.

    Authority detection priority:
    1. CONTROL_PLANE_REGION environment variable (if set)
    2. Cached authority from blob storage (from previous detection)
    3. Probe public -> government -> China clouds until subscriptions are found
    """
    # If region is explicitly set, use it directly
    region = environ.get(CONTROL_PLANE_REGION_SETTING)
    if region:
        return DefaultAzureCredential(authority=get_authority_for_region(region))

    # Check for cached authority
    cached_authority = await read_cache(AUTHORITY_CACHE_BLOB)
    if cached_authority and cached_authority in AUTHORITIES_TO_PROBE:
        log.debug("Using cached authority: %s", cached_authority)
        return DefaultAzureCredential(authority=cached_authority)

    # Probe and cache the authority
    detected_authority = await _detect_and_cache_authority()
    return DefaultAzureCredential(authority=detected_authority)
>>>>>>> Stashed changes


def get_event_hub_name(config_id: str) -> str:  # pragma: no cover
    return EVENT_HUB_NAME_PREFIX + config_id  # type: ignore


def get_event_hub_namespace(config_id: str) -> str:  # pragma: no cover
    return EVENT_HUB_NAMESPACE_PREFIX + config_id  # type: ignore


def resource_tag_dict_to_list(tags_dict: dict[str, str] | None) -> list[str]:
    """Convert a dictionary of Azure resource tags to a list of normalized tag strings"""
    tag_list = []
    for k, v in (tags_dict or {}).items():
        tag = k.strip().casefold()
        if v.strip():
            tag += f":{v.strip().casefold()}"
        tag_list.append(tag)

    return tag_list


def now() -> str:
    """Return the current time in ISO format"""
    return datetime.now().isoformat()


def average(*items: float, default: float = inf) -> float:
    """Return the average of the items, or `default` if no items are provided"""
    if not items:
        return default
    return sum(items) / len(items)


T = TypeVar("T")


def generate_unique_id() -> str:
    """Generate a unique ID which is 12 characters long using hex characters

    Example:
    >>> generate_unique_id()
    "c5653797a664"
    """
    return str(uuid4())[-12:]


def chunks(lst: list[T], n: int) -> Iterable[tuple[T, ...]]:
    """Yield successive n-sized chunks from lst. If the last chunk is smaller than n, it will be dropped"""
    return zip(*(lst[i::n] for i in range(n)), strict=False)


def log_errors(
    log: Logger,
    message: str,
    *maybe_errors: object | Exception,
    reraise: bool = False,
    extra: Mapping[str, str] | None = None,
) -> list[Exception]:
    """Log and return any errors in `maybe_errors`.
    If reraise is True, the first error will be raised"""
    errors = [e for e in maybe_errors if isinstance(e, Exception)]
    if errors:
        log.exception("%s: %s", message, errors, extra=extra)
        if reraise:
            raise errors[0]

    return errors


class Resource(Protocol):
    """Azure resource names are a string, useful for type casting"""

    name: str
