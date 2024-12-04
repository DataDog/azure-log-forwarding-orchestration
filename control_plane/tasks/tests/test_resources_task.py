# stdlib
from collections.abc import AsyncIterable, Callable
from json import dumps
from typing import Any, TypeAlias
from unittest.mock import AsyncMock, MagicMock, Mock

# project
from cache.resources_cache import RESOURCE_CACHE_BLOB, ResourceCache, deserialize_resource_cache
from tasks.constants import ALLOWED_RESOURCE_TYPES
from tasks.resources_task import RESOURCES_TASK_NAME, ResourcesTask
from tasks.tests.common import TaskTestCase, UnexpectedException, async_generator, mock

AsyncIterableFunc: TypeAlias = Callable[[], AsyncIterable[Mock]]

sub_id1 = "a062baee-fdd3-4784-beb4-d817f591422c"
sub_id2 = "77602a31-36b2-4417-a27c-9071107ca3e6"
sub1 = mock(subscription_id=sub_id1)
sub2 = mock(subscription_id=sub_id2)

resource1 = mock(id="res1", name="1", location="norwayeast", type="Microsoft.Compute/virtualMachines")
resource2 = mock(id="res2", name="2", location="norwayeast", type="Microsoft.Network/applicationgateways")
resource3 = mock(id="res3", name="3", location="southafricanorth", type="Microsoft.Network/loadBalancers")


class TestResourcesTask(TaskTestCase):
    TASK_NAME = RESOURCES_TASK_NAME

    def setUp(self) -> None:
        super().setUp()
        self.patch("get_config_option").side_effect = {
            "MONITORED_SUBSCRIPTIONS": '["a062baee-fdd3-4784-beb4-d817f591422c", "77602a31-36b2-4417-a27c-9071107ca3e6"]'
        }.__getitem__
        self.sub_client: AsyncMock = self.patch("SubscriptionClient").return_value.__aenter__.return_value
        self.resource_client = self.patch("ResourceManagementClient")
        self.resource_client_mapping: dict[str, AsyncIterableFunc] = {}

        def create_resource_client(_: Any, sub_id: str):
            c = MagicMock()
            assert sub_id in self.resource_client_mapping, "subscription not mocked properly"
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
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: Mock(return_value=async_generator(resource1, resource2)),
            sub_id2: Mock(return_value=async_generator(resource3)),
        }

        async with ResourcesTask("[[[[{{{{{asjdklahjs]]]}}}") as task:
            await task.run()

        self.log.warning.assert_called_once_with("Resource Cache is in an invalid format, task will reset the cache")
        self.assertEqual(
            self.cache, {sub_id1: {"norwayeast": {"res1", "res2"}}, sub_id2: {"southafricanorth": {"res3"}}}
        )

    async def test_empty_cache_adds_resources(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: Mock(return_value=async_generator(resource1, resource2)),
            sub_id2: Mock(return_value=async_generator(resource3)),
        }

        async with ResourcesTask("") as task:
            await task.run()

        self.log.warning.assert_called_once_with("Resource Cache is in an invalid format, task will reset the cache")
        self.assertEqual(
            self.cache, {sub_id1: {"norwayeast": {"res1", "res2"}}, sub_id2: {"southafricanorth": {"res3"}}}
        )

    async def test_no_new_resources_doesnt_cache(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: Mock(return_value=async_generator(resource1, resource2)),
            sub_id2: Mock(return_value=async_generator(resource3)),
        }
        await self.run_resources_task(
            {
                sub_id1: {"norwayeast": {"res1", "res2"}},
                sub_id2: {"southafricanorth": {"res3"}},
            }
        )

        self.write_cache.assert_not_called()

    async def test_resources_gone(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: Mock(return_value=async_generator()),
            sub_id2: Mock(return_value=async_generator()),
        }
        await self.run_resources_task(
            {
                sub_id1: {"southafricanorth": {"res1", "res2"}},
                sub_id2: {"norwayeast": {"res3"}},
            }
        )
        self.assertEqual(self.cache, {})

    async def test_subscriptions_gone(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator())
        # we dont return any subscriptions, so we should never call the resource client, if we do, it will error
        await self.run_resources_task(
            {
                sub_id1: {"norwayeast": {"res1", "res2"}},
                sub_id2: {"southafricanorth": {"res3"}},
            }
        )
        self.assertEqual(self.cache, {})

    async def test_global_resource_ignored(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: Mock(
                return_value=async_generator(
                    mock(id="res1", location="global", type="Microsoft.Compute/virtualMachines"), resource2
                )
            ),
            sub_id2: Mock(return_value=async_generator(resource3)),
        }
        await self.run_resources_task({})
        self.assertEqual(self.cache, {sub_id1: {"norwayeast": {"res2"}}, sub_id2: {"southafricanorth": {"res3"}}})

    async def test_unsupported_resource_types_ignored(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: Mock(
                return_value=async_generator(
                    mock(id="res1", location="norwayeast", type="Microsoft.Compute/Snapshots"),
                    resource2,
                )
            ),
            sub_id2: Mock(
                return_value=async_generator(
                    mock(id="res3", location="southafricanorth", type="Microsoft.AlertsManagement/PrometheusRuleGroups")
                )
            ),
        }
        await self.run_resources_task({})
        self.assertEqual(self.cache, {sub_id1: {"norwayeast": {"res2"}}})

    async def test_unexpected_failure_skips_cache_write(self):
        write_caches = self.patch("ResourcesTask.write_caches")
        self.sub_client.subscriptions.list = Mock(side_effect=UnexpectedException("unexpected"))
        with self.assertRaises(UnexpectedException):
            await self.run_resources_task(
                {
                    sub_id1: {"norwayeast": {"res1", "res2"}},
                    sub_id2: {"southafricanorth": {"res3"}},
                }
            )
        write_caches.assert_not_awaited()

    async def test_case_insensitive_matching(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: Mock(
                return_value=async_generator(
                    mock(id="rEs1", name="1", location="norwayeast", type="Microsoft.Compute/virtualMachines"),
                    mock(id="RES2", name="2", location="NORWAYEAST", type="Microsoft.Compute/virtualMachines"),
                )
            ),
            sub_id2: Mock(
                return_value=async_generator(
                    mock(id="reß3", name="3", location="southafricanorth", type="Microsoft.Compute/virtualMachines"),
                    mock(id="Res4", name="4", location="southafricanorth", type="Microsoft.Compute/virtualMachines"),
                )
            ),
        }

        await self.run_resources_task(
            {
                sub_id1: {"norwayeast": {"res1", "res2"}},
                sub_id2: {"southafricanorth": {"ress3"}},
            }
        )

        self.assertEqual(
            self.cache,
            {
                sub_id1: {"norwayeast": {"res1", "res2"}},
                sub_id2: {"southafricanorth": {"ress3", "res4"}},
            },
        )

    async def test_unmonitored_subscriptions_ignored(self):
        sub_id3 = "6522f787-edd0-4005-a901-d61c0ee60cb8"
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, Mock(subscription_id=sub_id3)))
        self.resource_client_mapping = {
            sub_id1: Mock(return_value=async_generator(resource1, resource2)),
            sub_id3: Mock(return_value=async_generator(resource3)),
        }
        await self.run_resources_task({})
        self.assertEqual(self.cache, {sub_id1: {"norwayeast": {"res1", "res2"}}})

    async def test_lfo_resource_is_ignored(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: Mock(
                return_value=async_generator(
                    mock(
                        id="/subscriptions/whatever/whatever/dd-lfo-control-12983471",
                        name="scaling-task-12983471",
                        location="norwayeast",
                        type="Microsoft.Web/sites",
                    ),
                    resource1,
                )
            ),
            sub_id2: Mock(
                return_value=async_generator(
                    resource3,
                    mock(
                        id="/subscriptions/whatever/whatever/dd-log-forwarder-env-12314535",
                        name="dd-log-forwarder-env-12314535",
                        location="southafricanorth",
                        type="Microsoft.App/managedEnvironments",
                    ),
                )
            ),
        }

        await self.run_resources_task({})

        self.assertEqual(
            self.cache,
            {
                sub_id1: {"norwayeast": {"res1"}},
                sub_id2: {"southafricanorth": {"res3"}},
            },
        )

    async def test_resource_query_filter(self):
        queryFilter = ResourcesTask.resource_query_filter(ALLOWED_RESOURCE_TYPES)

        for rt in ALLOWED_RESOURCE_TYPES:
            self.assertTrue(f"resourceType eq '{rt}'" in queryFilter)

        self.assertEqual(queryFilter.count("or resourceType"), len(ALLOWED_RESOURCE_TYPES) - 1)
