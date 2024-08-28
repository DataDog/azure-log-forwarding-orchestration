# stdlib
from collections.abc import AsyncIterable, Callable
from json import dumps
from typing import Any, TypeAlias
from unittest.mock import AsyncMock, MagicMock, Mock

# project
from cache.resources_cache import RESOURCE_CACHE_BLOB, ResourceCache, deserialize_resource_cache
from tasks.resources_task import RESOURCES_TASK_NAME, ResourcesTask
from tasks.tests.common import TaskTestCase, UnexpectedException, async_generator, mock

AsyncIterableFunc: TypeAlias = Callable[[], AsyncIterable[Mock]]


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

    @property
    def cache(self) -> ResourceCache:
        return self.cache_value(RESOURCE_CACHE_BLOB, deserialize_resource_cache)

    async def test_invalid_cache(self):
        self.sub_client.subscriptions.list = Mock(
            return_value=async_generator(Mock(subscription_id="sub1"), Mock(subscription_id="sub2"))
        )
        self.resource_client_mapping = {
            "sub1": Mock(
                return_value=async_generator(mock(id="res1", location="region1"), mock(id="res2", location="region1"))
            ),
            "sub2": Mock(return_value=async_generator(mock(id="res3", location="region2"))),
        }

        async with ResourcesTask("[[[[{{{{{asjdklahjs]]]}}}") as task:
            await task.run()

        self.log.warning.assert_called_once_with("Resource Cache is in an invalid format, task will reset the cache")
        self.assertEqual(self.cache, {"sub1": {"region1": {"res1", "res2"}}, "sub2": {"region2": {"res3"}}})

    async def test_empty_cache_adds_resources(self):
        self.sub_client.subscriptions.list = Mock(
            return_value=async_generator(Mock(subscription_id="sub1"), Mock(subscription_id="sub2"))
        )
        self.resource_client_mapping = {
            "sub1": Mock(
                return_value=async_generator(mock(id="res1", location="region1"), mock(id="res2", location="region1"))
            ),
            "sub2": Mock(return_value=async_generator(mock(id="res3", location="region2"))),
        }

        async with ResourcesTask("") as task:
            await task.run()

        self.log.warning.assert_called_once_with("Resource Cache is in an invalid format, task will reset the cache")
        self.assertEqual(self.cache, {"sub1": {"region1": {"res1", "res2"}}, "sub2": {"region2": {"res3"}}})

    async def test_no_new_resources_doesnt_cache(self):
        self.sub_client.subscriptions.list = Mock(
            return_value=async_generator(Mock(subscription_id="sub1"), Mock(subscription_id="sub2"))
        )
        self.resource_client_mapping = {
            "sub1": Mock(
                return_value=async_generator(mock(id="res1", location="region1"), mock(id="res2", location="region1"))
            ),
            "sub2": Mock(return_value=async_generator(mock(id="res3", location="region2"))),
        }
        await self.run_resources_task(
            {
                "sub1": {"region1": {"res1", "res2"}},
                "sub2": {"region2": {"res3"}},
            }
        )

        self.write_cache.assert_not_called()

    async def test_resources_gone(self):
        self.sub_client.subscriptions.list = Mock(
            return_value=async_generator(Mock(subscription_id="sub1"), Mock(subscription_id="sub2"))
        )
        self.resource_client_mapping = {
            "sub1": Mock(return_value=async_generator()),
            "sub2": Mock(return_value=async_generator()),
        }
        await self.run_resources_task(
            {
                "sub1": {"region2": {"res1", "res2"}},
                "sub2": {"region1": {"res3"}},
            }
        )
        self.assertEqual(self.cache, {"sub1": {}, "sub2": {}})

    async def test_subscriptions_gone(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator())
        # we dont return any subscriptions, so we should never call the resource client, if we do, it will error
        await self.run_resources_task(
            {
                "sub1": {"region1": {"res1", "res2"}},
                "sub2": {"region2": {"res3"}},
            }
        )
        self.assertEqual(self.cache, {})

    async def test_global_resource_ignored(self):
        self.sub_client.subscriptions.list = Mock(
            return_value=async_generator(Mock(subscription_id="sub1"), Mock(subscription_id="sub2"))
        )
        self.resource_client_mapping = {
            "sub1": Mock(
                return_value=async_generator(mock(id="res1", location="global"), mock(id="res2", location="region1"))
            ),
            "sub2": Mock(return_value=async_generator(mock(id="res3", location="region2"))),
        }
        await self.run_resources_task({})
        self.assertEqual(self.cache, {"sub1": {"region1": {"res2"}}, "sub2": {"region2": {"res3"}}})

    async def test_unsupported_resource_types_ignored(self):
        self.sub_client.subscriptions.list = Mock(
            return_value=async_generator(Mock(subscription_id="sub1"), Mock(subscription_id="sub2"))
        )
        self.resource_client_mapping = {
            "sub1": Mock(
                return_value=async_generator(
                    mock(id="res1", location="region1", type="Microsoft.Compute/Snapshots"),
                    mock(id="res2", location="region1", type="Microsoft.Compute/VirtualMachines"),
                )
            ),
            "sub2": Mock(
                return_value=async_generator(
                    mock(id="res3", location="region2", type="Microsoft.AlertsManagement/PrometheusRuleGroups")
                )
            ),
        }
        await self.run_resources_task({})
        self.assertEqual(self.cache, {"sub1": {"region1": {"res2"}}, "sub2": {}})

    async def test_unexpected_failure_skips_cache_write(self):
        write_caches = self.patch("ResourcesTask.write_caches")
        self.sub_client.subscriptions.list = Mock(side_effect=UnexpectedException("unexpected"))
        with self.assertRaises(UnexpectedException):
            await self.run_resources_task(
                {
                    "sub1": {"region1": {"res1", "res2"}},
                    "sub2": {"region2": {"res3"}},
                }
            )
        write_caches.assert_not_awaited()
