# stdlib
from collections.abc import AsyncIterable, Callable
from json import dumps
from typing import Any, TypeAlias
from unittest import TestCase
from unittest.mock import Mock

# project
from cache.resources_cache import RESOURCE_CACHE_BLOB, ResourceCache, deserialize_resource_cache
from tasks.constants import ALLOWED_RESOURCE_TYPES
from tasks.resources_task import RESOURCE_QUERY_FILTER, RESOURCES_TASK_NAME, ResourcesTask, should_ignore_resource
from tasks.tests.common import AsyncMockClient, TaskTestCase, UnexpectedException, async_generator, mock

AsyncIterableFunc: TypeAlias = Callable[[], AsyncIterable[Mock]]

sub_id1 = "a062baee-fdd3-4784-beb4-d817f591422c"
sub_id2 = "77602a31-36b2-4417-a27c-9071107ca3e6"
sub1 = mock(subscription_id=sub_id1)
sub2 = mock(subscription_id=sub_id2)

SUPPORTED_REGION_1 = "norwayeast"
SUPPORTED_REGION_2 = "southafricanorth"
CONTAINER_APPS_UNSUPPORTED_REGION = "newzealandnorth"
UNSUPPORTED_REGION = "uae"
resource1 = mock(id="res1", name="1", location=SUPPORTED_REGION_1, type="Microsoft.Compute/virtualMachines")
resource2 = mock(id="res2", name="2", location=SUPPORTED_REGION_1, type="Microsoft.Network/applicationgateways")
resource3 = mock(id="res3", name="3", location=SUPPORTED_REGION_2, type="Microsoft.Network/loadBalancers")


class TestResourcesTask(TaskTestCase):
    TASK_NAME = RESOURCES_TASK_NAME

    def setUp(self) -> None:
        super().setUp()
        self.sub_client = AsyncMockClient()
        self.patch("SubscriptionClient").return_value = self.sub_client
        self.resource_client = self.patch("ResourceManagementClient")
        self.resource_client_mapping: dict[str, AsyncIterableFunc] = {}

        def create_resource_client(_: Any, sub_id: str):
            c = AsyncMockClient()
            assert sub_id in self.resource_client_mapping, "subscription not mocked properly"
            c.resources = Mock(list=self.resource_client_mapping[sub_id])
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
            self.cache, {sub_id1: {SUPPORTED_REGION_1: {"res1", "res2"}}, sub_id2: {SUPPORTED_REGION_2: {"res3"}}}
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
            self.cache, {sub_id1: {SUPPORTED_REGION_1: {"res1", "res2"}}, sub_id2: {SUPPORTED_REGION_2: {"res3"}}}
        )

    async def test_no_new_resources_doesnt_cache(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: Mock(return_value=async_generator(resource1, resource2)),
            sub_id2: Mock(return_value=async_generator(resource3)),
        }
        await self.run_resources_task(
            {
                sub_id1: {SUPPORTED_REGION_1: {"res1", "res2"}},
                sub_id2: {SUPPORTED_REGION_2: {"res3"}},
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
                sub_id1: {SUPPORTED_REGION_2: {"res1", "res2"}},
                sub_id2: {SUPPORTED_REGION_1: {"res3"}},
            }
        )
        self.assertEqual(self.cache, {})

    async def test_subscriptions_gone(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator())
        # we dont return any subscriptions, so we should never call the resource client, if we do, it will error
        await self.run_resources_task(
            {
                sub_id1: {SUPPORTED_REGION_1: {"res1", "res2"}},
                sub_id2: {SUPPORTED_REGION_2: {"res3"}},
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
        self.assertEqual(self.cache, {sub_id1: {SUPPORTED_REGION_1: {"res2"}}, sub_id2: {SUPPORTED_REGION_2: {"res3"}}})

    async def test_unsupported_resource_types_ignored(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: Mock(
                return_value=async_generator(
                    mock(id="res1", location=SUPPORTED_REGION_1, type="Microsoft.Compute/Snapshots"),
                    resource2,
                )
            ),
            sub_id2: Mock(
                return_value=async_generator(
                    mock(
                        id="res3",
                        location=SUPPORTED_REGION_2,
                        type="Microsoft.AlertsManagement/PrometheusRuleGroups",
                    )
                )
            ),
        }
        await self.run_resources_task({})
        self.assertEqual(self.cache, {sub_id1: {SUPPORTED_REGION_1: {"res2"}}})

    async def test_unexpected_failure_skips_cache_write(self):
        write_caches = self.patch("ResourcesTask.write_caches")
        self.sub_client.subscriptions.list = Mock(side_effect=UnexpectedException("unexpected"))
        with self.assertRaises(UnexpectedException):
            await self.run_resources_task(
                {
                    sub_id1: {SUPPORTED_REGION_1: {"res1", "res2"}},
                    sub_id2: {SUPPORTED_REGION_2: {"res3"}},
                }
            )
        write_caches.assert_not_awaited()

    async def test_case_insensitive_matching(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: Mock(
                return_value=async_generator(
                    mock(id="rEs1", name="1", location=SUPPORTED_REGION_1, type="Microsoft.Compute/virtualMachines"),
                    mock(id="RES2", name="2", location=SUPPORTED_REGION_1, type="Microsoft.Compute/virtualMachines"),
                )
            ),
            sub_id2: Mock(
                return_value=async_generator(
                    mock(id="reß3", name="3", location=SUPPORTED_REGION_2, type="Microsoft.Compute/virtualMachines"),
                    mock(id="Res4", name="4", location=SUPPORTED_REGION_2, type="Microsoft.Compute/virtualMachines"),
                )
            ),
        }

        await self.run_resources_task(
            {
                sub_id1: {SUPPORTED_REGION_1: {"res1", "res2"}},
                sub_id2: {SUPPORTED_REGION_2: {"reß3"}},
            }
        )

        self.assertEqual(
            self.cache,
            {
                sub_id1: {SUPPORTED_REGION_1: {"res1", "res2"}},
                sub_id2: {SUPPORTED_REGION_2: {"reß3", "res4"}},
            },
        )

    async def test_unmonitored_subscriptions_ignored(self):
        sub_id3 = "6522f787-edd0-4005-a901-d61c0ee60cb8"
        self.patch("getenv").side_effect = {
            "MONITORED_SUBSCRIPTIONS": '["a062baee-fdd3-4784-beb4-d817f591422c", "77602a31-36b2-4417-a27c-9071107ca3e6"]'
        }.__getitem__
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, Mock(subscription_id=sub_id3)))
        self.resource_client_mapping = {
            sub_id1: Mock(return_value=async_generator(resource1, resource2)),
            sub_id3: Mock(return_value=async_generator(resource3)),
        }
        await self.run_resources_task({})
        self.assertEqual(self.cache, {sub_id1: {SUPPORTED_REGION_1: {"res1", "res2"}}})

    async def test_lfo_resource_is_ignored(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: Mock(
                return_value=async_generator(
                    mock(
                        id="/subscriptions/whatever/whatever/dd-lfo-control-12983471",
                        name="scaling-task-12983471",
                        location=SUPPORTED_REGION_1,
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
                        location=SUPPORTED_REGION_2,
                        type="Microsoft.App/managedEnvironments",
                    ),
                )
            ),
        }

        await self.run_resources_task({})

        self.assertEqual(
            self.cache,
            {
                sub_id1: {SUPPORTED_REGION_1: {"res1"}},
                sub_id2: {SUPPORTED_REGION_2: {"res3"}},
            },
        )

    async def test_resource_query_filter(self):
        for rt in ALLOWED_RESOURCE_TYPES:
            self.assertTrue(f"resourceType eq '{rt}'" in RESOURCE_QUERY_FILTER)

        self.assertEqual(
            RESOURCE_QUERY_FILTER.count("or resourceType"),
            len(ALLOWED_RESOURCE_TYPES) - 1,
        )


class TestResourceTaskHelpers(TestCase):
    def test_should_ignore_resource_by_region(self):
        vm_type = "Microsoft.Compute/virtualMachines"
        vm_name = "vm1"
        # valid regions
        self.assertFalse(should_ignore_resource(SUPPORTED_REGION_1, vm_type, vm_name))
        self.assertFalse(should_ignore_resource(SUPPORTED_REGION_2, vm_type, vm_name))
        self.assertFalse(should_ignore_resource(CONTAINER_APPS_UNSUPPORTED_REGION, vm_type, vm_name))

        # invalid regions
        self.assertTrue(should_ignore_resource(UNSUPPORTED_REGION, vm_type, vm_name))
        self.assertTrue(should_ignore_resource("nonsense region that doenst exist", vm_type, vm_name))
        self.assertTrue(should_ignore_resource("global", vm_type, vm_name))

    def test_should_ignore_resource_by_type(self):
        # valid types
        self.assertFalse(should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.Compute/virtualMachines", "vm1"))
        self.assertFalse(should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.Network/applicationGateways", "ag1"))

        # invalid types
        self.assertTrue(should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.Compute/Snapshots", "snap1"))
        self.assertTrue(
            should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.AlertsManagement/PrometheusRuleGroups", "prg1")
        )

    def test_should_ignore_resource_by_name(self):
        # normal resources
        self.assertFalse(should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.Compute/virtualMachines", "vm1"))
        self.assertFalse(should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.Compute/virtualMachines", "vm2"))

        # TODO (AZINTS-2763): ensure storage accounts are ignored
        # control plane resources

        self.assertTrue(
            should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.App/managedEnvironments", "dd-log-forwarder-env-")
        )
        self.assertTrue(should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.Web/sites", "scaling-task-"))
        self.assertTrue(should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.Web/sites", "resources-task-"))
        self.assertTrue(should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.Web/sites", "diagnostic-settings-task-"))
