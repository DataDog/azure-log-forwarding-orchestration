# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from collections.abc import AsyncIterable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, TypeVar
from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, call, patch

# project
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

    def patch(self, obj: str, **kwargs: Any):
        return self.patch_path(f"tasks.{self.TASK_NAME}.{obj}", **kwargs)

    def setUp(self) -> None:
        cred_mock = self.patch_path("tasks.task.DefaultAzureCredential", return_value=AsyncMockClient())
        self.credential = cred_mock.return_value
        self.credential.side_effect = AsyncMock
        self.datadog_api_client = self.patch_path("tasks.task.AsyncApiClient", return_value=AsyncMockClient())
        self.datadog_logs_api = self.patch_path("tasks.task.LogsApi", return_value=AsyncMock())
        self.datadog_metrics_api = self.patch_path("tasks.task.MetricsApi", return_value=AsyncMock())
        self.env = {}
        task_env_mock = self.patch_path("tasks.task.environ", create=True)
        task_env_mock.get.side_effect = lambda k, default="unset test env var": self.env.get(k, default)
        env_env_mock = self.patch_path("cache.env.environ", create=True)
        env_env_mock.get.side_effect = lambda k, default="unset test env var": self.env.get(k, default)

        with suppress(AttributeError):
            self.write_cache: AsyncMock = self.patch("write_cache")

    def cache_value(self, cache_name: str, deserialize_cache: Callable[[str], T | None]) -> T:
        self.write_cache.assert_called_with(cache_name, ANY)
        raw_cache = self.write_cache.call_args_list[-1][0][1]
        cache = deserialize_cache(raw_cache)
        if cache is None:  # pragma: no cover
            # should never happen when tests pass, but it provides a useful error message if they don't
            raise InvalidCacheError("Diagnostic Settings Cache is in an invalid format after the task")
        return cache


async def async_generator(*items: T) -> AsyncIterable[T]:
    for x in items:
        if isinstance(x, Exception):
            raise x
        yield x


class UnexpectedException(Exception):
    """Testing for exceptions that we havent accounted for"""

    pass


def mock(**kwargs: Any) -> Mock:
    m = Mock()
    for k, v in kwargs.items():
        setattr(m, k, v)
    return m


def AsyncMockClient(**kwargs: Any) -> AsyncMock:
    """An AsyncMock with the context manager methods set up to use as a client"""
    m = AsyncMock(**kwargs)
    m.__aenter__.return_value = m
    m.__aexit__.return_value = None
    return m


@dataclass(frozen=True)
class AzureModelMatcher:
    expected: dict[str, Any]

    def __eq__(self, other: Any) -> bool:
        with suppress(Exception):
            return other.as_dict() == self.expected
        return False  # pragma: no cover
