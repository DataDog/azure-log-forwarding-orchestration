# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from asyncio import Task, create_task
from collections.abc import AsyncIterable, Awaitable
from logging import Logger
from typing import TypeVar

T = TypeVar("T")


async def collect(it: AsyncIterable[T]) -> list[T]:
    """Helper for collecting an async iterable, useful for simplifying error handling"""
    return [item async for item in it]


async def safe_collect(it: AsyncIterable[T], log: Logger) -> list[T]:
    """Helper for collecting an async iterable, logs a warning if an error was thrown but returns the result so far"""
    collected = []
    try:
        async for item in it:
            collected.append(item)
    except Exception as e:
        log.warning("Ignored error while collecting async iterable: %s", e)
    return collected


def create_task_from_awaitable(awaitable: Awaitable[T]) -> Task[T]:
    """Turns an awaitable object into an asyncio Task,
    which starts it immediately and it can be awaited later"""

    async def _f() -> T:
        return await awaitable

    return create_task(_f())
