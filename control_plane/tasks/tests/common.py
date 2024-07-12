from collections.abc import AsyncIterable
from typing import Any
from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, patch


class AsyncTestCase(IsolatedAsyncioTestCase):
    def patch_path(self, path: str, **kwargs: Any):
        p = patch(path, **kwargs)
        self.addCleanup(p.stop)
        return p.start()


class TaskTestCase(AsyncTestCase):
    TASK_NAME: str = NotImplemented

    def patch(self, obj: str):
        return self.patch_path(f"tasks.{self.TASK_NAME}.{obj}")

    def setUp(self) -> None:
        self.credential = self.patch_path("tasks.task.DefaultAzureCredential")
        self.credential.side_effect = AsyncMock
        self.write_cache: AsyncMock = self.patch("write_cache")

    def cache_value(self, cache_name: str) -> str:
        self.write_cache.assert_called_with(cache_name, ANY)
        return self.write_cache.call_args_list[-1][0][1]


async def async_generator[T](*items: T) -> AsyncIterable[T]:
    for x in items:
        yield x
