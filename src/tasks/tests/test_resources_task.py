from json import dumps
from typing import Any, AsyncIterable, Callable, TypeAlias, TypeVar
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from resources_task.function_app import ResourcesTask, ResourceCache, deserialize_resource_cache
from unittest import IsolatedAsyncioTestCase


T = TypeVar("T")

AsyncIterableFunc: TypeAlias = Callable[[], AsyncIterable[Mock]]


async def agen(*items: T) -> AsyncIterable[T]:
    for x in items:
        yield x


def make_agen_func(field_name: str, *values: str) -> AsyncIterableFunc:
    """useful wrapper for client methods which return an `AsyncIterable` of objects with a single field."""
    return Mock(return_value=agen(*(Mock(**{field_name: value}) for value in values)))


class TestResourcesTask(IsolatedAsyncioTestCase):
    def patch(self, path: str):
        p = patch(f"resources_task.function_app.{path}")
        self.addCleanup(p.stop)
        return p.start()

    def setUp(self) -> None:
        self.sub_client: AsyncMock = self.patch("SubscriptionClient").return_value.__aenter__.return_value
        self.resource_client = self.patch("ResourceManagementClient")
        self.resource_client_mapping: dict[str, AsyncIterableFunc] = {}

        def create_resource_client(_: Any, sub_id: str):
            c = MagicMock()
            c.__aenter__.return_value.resources.list = self.resource_client_mapping[sub_id]
            return c

        self.resource_client.side_effect = create_resource_client
        self.credential = AsyncMock()
        self.out_mock = Mock()

        self.log = self.patch("log")

    @property
    def out_value(self) -> ResourceCache:
        return deserialize_resource_cache(self.out_mock.set.call_args[0][0])[1]

    def run_resources_task(self, cache: ResourceCache):
        return ResourcesTask(self.credential, dumps(cache, default=list), self.out_mock).run()

    async def test_invalid_cache(self):
        self.sub_client.subscriptions.list = make_agen_func("subscription_id", "sub1", "sub2")
        self.resource_client_mapping = {
            "sub1": make_agen_func("id", "res1", "res2"),
            "sub2": make_agen_func("id", "res3"),
        }
        await ResourcesTask(self.credential, "[[[[{{{{{asjdklahjs]]]}}}", self.out_mock).run()

        self.log.warning.assert_called_once_with("Resource Cache is in an invalid format, task will reset the cache")
        self.assertEqual(self.out_value, {"sub1": {"res1", "res2"}, "sub2": {"res3"}})

    async def test_empty_cache_adds_resources(self):
        self.sub_client.subscriptions.list = make_agen_func("subscription_id", "sub1", "sub2")
        self.resource_client_mapping = {
            "sub1": make_agen_func("id", "res1", "res2"),
            "sub2": make_agen_func("id", "res3"),
        }

        await ResourcesTask(self.credential, "", self.out_mock).run()

        self.log.warning.assert_called_once_with("Resource Cache is in an invalid format, task will reset the cache")
        self.assertEqual(self.out_value, {"sub1": {"res1", "res2"}, "sub2": {"res3"}})

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

        self.out_mock.set.assert_not_called()

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
        self.assertEqual(self.out_value, {"sub1": set(), "sub2": set()})

    async def test_subscriptions_gone(self):
        self.sub_client.subscriptions.list = make_agen_func("subscription_id")
        # we dont return any subscriptions, so we should never call the resource client, if we do, it will error
        await self.run_resources_task(
            {
                "sub1": {"res1", "res2"},
                "sub2": {"res3"},
            }
        )
        self.assertEqual(self.out_value, {})
