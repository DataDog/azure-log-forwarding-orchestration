# stdlib
from collections.abc import AsyncIterable, Callable
from typing import Any, TypeVar
from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, MagicMock, call, patch

from cache.common import InvalidCacheError


class AsyncTestCase(IsolatedAsyncioTestCase):
    def patch_path(self, path: str, **kwargs: Any) -> MagicMock | AsyncMock:
        p = patch(path, **kwargs)
        self.addCleanup(p.stop)
        return p.start()

    def assertCalledTimesWith(self, mock: AsyncMock, times: int, /, *args: Any, **kwargs: Any):
        self.assertEqual(mock.await_count, times)
        self.assertEqual([call(*args, **kwargs)] * times, mock.await_args_list)


T = TypeVar("T")


class TaskTestCase(AsyncTestCase):
    TASK_NAME: str = NotImplemented

    def patch(self, obj: str):
        return self.patch_path(f"tasks.{self.TASK_NAME}.{obj}")

    def setUp(self) -> None:
        self.credential = self.patch_path("tasks.task.DefaultAzureCredential")
        self.credential.side_effect = AsyncMock
        self.write_cache: AsyncMock = self.patch("write_cache")

    def cache_value(self, cache_name: str, deserialize_cache: Callable[[str], tuple[bool, T]]) -> T:
        self.write_cache.assert_called_with(cache_name, ANY)
        raw_cache = self.write_cache.call_args_list[-1][0][1]
        success, cache = deserialize_cache(raw_cache)
        if not success:  # pragma: no cover
            # should never happen when tests pass, but it provides a useful error message if they don't
            raise InvalidCacheError("Diagnostic Settings Cache is in an invalid format after the task")
        return cache


async def async_generator(*items: T) -> AsyncIterable[T]:
    for x in items:
        yield x


class UnexpectedException(Exception):
    """Testing for exceptions that we havent accounted for"""

    pass
