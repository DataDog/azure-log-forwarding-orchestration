# stdlib
from collections.abc import Awaitable, Callable
from datetime import datetime
from math import inf
from typing import Any, TypeVar
from uuid import uuid4

# 3p
from azure.core.exceptions import ResourceNotFoundError
from azure.core.polling import AsyncLROPoller
from tenacity import retry, retry_if_exception_type, stop_after_delay


def now() -> str:
    """Return the current time in ISO format"""
    return datetime.now().isoformat()


def average(*items: float, default: float = inf) -> float:
    """Return the average of the items, or `default` if no items are provided"""
    if not items:
        return default
    return sum(items) / len(items)


T = TypeVar("T")


async def wait_for_resource(
    poller: AsyncLROPoller[T], confirm: Callable[[], Awaitable[Any]], wait_seconds: int = 30
) -> T:
    """Wait for the poller to complete and confirm the resource exists,
    if the resource does not exist, `confirm` should throw a ResourceNotFoundError"""
    res = await poller.result()

    await retry(
        retry=retry_if_exception_type(ResourceNotFoundError),
        stop=stop_after_delay(wait_seconds),
    )(confirm)()

    return res


def generate_unique_id() -> str:
    """Generate a unique ID which is 12 characters long using hex characters

    Example:
    >>> generate_unique_id()
    "c5653797a664"
    """
    return str(uuid4())[:12]
