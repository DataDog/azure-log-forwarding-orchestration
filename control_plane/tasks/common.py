# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/  Copyright 2025 Datadog, Inc.

# stdlib
from collections.abc import Iterable, Mapping
from datetime import datetime
from logging import Logger
from math import inf
from typing import Final, Protocol, TypeVar
from uuid import uuid4

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
    return region.startswith("usgov")


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
