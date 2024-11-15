# stdlib
from collections.abc import AsyncIterable, Iterable
from datetime import datetime
from logging import getLogger
from math import inf
from typing import Final, Protocol, TypeVar
from uuid import uuid4

log = getLogger(__name__)

CONTROL_PLANE_APP_SERVICE_PLAN_PREFIX: Final = "dd-lfo-control-"
CONTROL_PLANE_STORAGE_PREFIX: Final = "ddlfocontrol"
SCALING_TASK_PREFIX: Final = "scaling-task-"
RESOURCES_TASK_PREFIX: Final = "resources-task-"
DIAGNOSTIC_SETTINGS_TASK_PREFIX: Final = "diagnostic-settings-task-"


CONTAINER_APP_PREFIX: Final = "dd-log-forwarder-"
MANAGED_ENVIRONMENT_PREFIX: Final = "dd-log-forwarder-env-"
STORAGE_ACCOUNT_PREFIX: Final = "ddlogstorage"


def get_container_app_name(config_id: str) -> str:
    return CONTAINER_APP_PREFIX + config_id


def get_resource_group_id(subscription_id: str, resource_group: str) -> str:
    return f"/subscriptions/{subscription_id}/resourcegroups/{resource_group}".casefold()


def get_container_app_id(subscription_id: str, resource_group: str, config_id: str) -> str:
    return (
        get_resource_group_id(subscription_id, resource_group)
        + "/providers/microsoft.app/jobs/"
        + get_container_app_name(config_id)
    ).casefold()


def get_managed_env_name(config_id: str) -> str:
    return MANAGED_ENVIRONMENT_PREFIX + config_id


def get_managed_env_id(subscription_id: str, resource_group: str, config_id: str) -> str:
    return (
        get_resource_group_id(subscription_id, resource_group)
        + "/providers/microsoft.app/managedenvironments/"
        + get_managed_env_name(config_id)
    ).casefold()


def get_storage_account_name(config_id: str) -> str:
    return STORAGE_ACCOUNT_PREFIX + config_id


def get_storage_account_id(subscription_id: str, resource_group: str, config_id: str) -> str:
    return (
        get_resource_group_id(subscription_id, resource_group)
        + "/providers/microsoft.storage/storageaccounts/"
        + get_storage_account_name(config_id)
    ).casefold()


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


async def collect(it: AsyncIterable[T]) -> list[T]:
    """Helper for collecting an async iterable, useful for simplifying error handling"""
    return [item async for item in it]


def chunks(lst: list[T], n: int) -> Iterable[tuple[T, ...]]:
    """Yield successive n-sized chunks from lst. If the last chunk is smaller than n, it will be dropped"""
    return zip(*(lst[i::n] for i in range(n)), strict=False)


def log_errors(message: str, *maybe_errors: object | Exception, reraise: bool = False) -> list[Exception]:
    """Log and return any errors in `maybe_errors`.
    If reraise is True, the first error will be raised"""
    errors = [e for e in maybe_errors if isinstance(e, Exception)]
    if errors:
        log.exception("%s: %s", message, errors)
        if reraise:
            raise errors[0]

    return errors


class Resource(Protocol):
    """Azure resource names are a string, useful for type casting"""

    name: str
