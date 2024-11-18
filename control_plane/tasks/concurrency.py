# stdlib
from asyncio import Task, create_task
from collections.abc import AsyncIterable, Awaitable
from typing import TypeVar

T = TypeVar("T")


async def collect(it: AsyncIterable[T]) -> list[T]:
    """Helper for collecting an async iterable, useful for simplifying error handling"""
    return [item async for item in it]


def create_task_from_awaitable(awaitable: Awaitable[T]) -> Task[T]:
    """Turns an awaitable object into an asyncio Task,
    which starts it immediately and it can be awaited later"""

    async def _f() -> T:
        return await awaitable

    return create_task(_f())
