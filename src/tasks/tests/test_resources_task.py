from json import dumps
from typing import Any, AsyncIterable, Callable, TypeAlias, TypeVar
from unittest.mock import AsyncMock, MagicMock, Mock

from src.cache.resources_cache import RESOURCE_CACHE_BLOB, ResourceCache, deserialize_resource_cache
from src.tasks.resources_task import RESOURCES_TASK_NAME, ResourcesTask
from src.tasks.tests.common import TaskTestCase


T = TypeVar("T")

AsyncIterableFunc: TypeAlias = Callable[[], AsyncIterable[Mock]]


async def agen(*items: T) -> AsyncIterable[T]:
    for x in items:
        yield x


def make_agen_func(field_name: str, *values: str) -> AsyncIterableFunc:
    """useful wrapper for client methods which return an `AsyncIterable` of objects with a single field."""
    return Mock(return_value=agen(*(Mock(**{field_name: value}) for value in values)))


class TestResourcesTask(TaskTestCase):
    TASK_NAME = RESOURCES_TASK_NAME

    def setUp(self) -> None:
        super().setUp()
        self.sub_client: AsyncMock = self.patch("SubscriptionClient").return_value.__aenter__.return_value
        self.resource_client = self.patch("ResourceManagementClient")
        self.resource_client_mapping: dict[str, AsyncIterableFunc] = {}

        def create_resource_client(_: Any, sub_id: str):
            c = MagicMock()
            c.__aenter__.return_value.resources.list = self.resource_client_mapping[sub_id]
            return c

        self.resource_client.side_effect = create_resource_client

        self.log = self.patch("log")

    async def run_resources_task(self, cache: ResourceCache):
        async with ResourcesTask(dumps(cache, default=list)) as task:
            await task.run()

    async def test_invalid_cache(self):
        self.sub_client.subscriptions.list = make_agen_func("subscription_id", "sub1", "sub2")
        self.resource_client_mapping = {
            "sub1": make_agen_func("id", "res1", "res2"),
            "sub2": make_agen_func("id", "res3"),
        }

        async with ResourcesTask("[[[[{{{{{asjdklahjs]]]}}}") as task:
            await task.run()

        self.log.warning.assert_called_once_with("Resource Cache is in an invalid format, task will reset the cache")
        self.assertEqual(
            deserialize_resource_cache(self.cache_value(RESOURCE_CACHE_BLOB))[1],
            {"sub1": {"res1", "res2"}, "sub2": {"res3"}},
        )

    async def test_empty_cache_adds_resources(self):
        self.sub_client.subscriptions.list = make_agen_func("subscription_id", "sub1", "sub2")
        self.resource_client_mapping = {
            "sub1": make_agen_func("id", "res1", "res2"),
            "sub2": make_agen_func("id", "res3"),
        }

        async with ResourcesTask("") as task:
            await task.run()

        self.log.warning.assert_called_once_with("Resource Cache is in an invalid format, task will reset the cache")
        self.assertEqual(
            deserialize_resource_cache(self.cache_value(RESOURCE_CACHE_BLOB))[1],
            {"sub1": {"res1", "res2"}, "sub2": {"res3"}},
        )

    async def test_no_new_resources_doesnt_cache(self):
        self.sub_client.subscriptions.list = make_agen_func("subscription_id", "sub1", "sub2")
        self.resource_client_mapping = {
            "sub1": make_agen_func("id", "res1", "res2"),
            "sub2": make_agen_func("id", "res3"),
        }

        await self.run_resources_task(
            {
                "sub1": {"res1", "res2"},
                "sub2": {"res3"},
            }
        )

        self.write_cache.assert_not_called()

    async def test_resources_gone(self):
        self.sub_client.subscriptions.list = make_agen_func("subscription_id", "sub1", "sub2")
        self.resource_client_mapping = {
            "sub1": make_agen_func("id"),
            "sub2": make_agen_func("id"),
        }
        await self.run_resources_task(
            {
                "sub1": {"res1", "res2"},
                "sub2": {"res3"},
            }
        )
        self.assertEqual(
            deserialize_resource_cache(self.cache_value(RESOURCE_CACHE_BLOB))[1], {"sub1": set(), "sub2": set()}
        )

    async def test_subscriptions_gone(self):
        self.sub_client.subscriptions.list = make_agen_func("subscription_id")
        # we dont return any subscriptions, so we should never call the resource client, if we do, it will error
        await self.run_resources_task(
            {
                "sub1": {"res1", "res2"},
                "sub2": {"res3"},
            }
        )
        self.assertEqual(deserialize_resource_cache(self.cache_value(RESOURCE_CACHE_BLOB))[1], {})
