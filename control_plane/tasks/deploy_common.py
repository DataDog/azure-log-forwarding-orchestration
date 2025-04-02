# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

# 3p
from azure.core.exceptions import ResourceNotFoundError
from azure.core.polling import AsyncLROPoller
from tenacity import retry, retry_if_exception_type, stop_after_delay

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
