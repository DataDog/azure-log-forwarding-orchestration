# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from json import dumps
from typing import Any
from unittest.mock import Mock, call
from uuid import uuid4

# 3p
from azure.core.exceptions import HttpResponseError

# project
from cache.env import (
    CONTROL_PLANE_ID_SETTING,
)
from cache.resources_cache import (
    RESOURCE_CACHE_BLOB,
    ResourceCache,
    ResourceCacheV1,
    ResourceMetadata,
    _deserialize_v2_resource_cache,
)
from tasks.client.datadog_api_client import StatusCode
from tasks.resources_task import RESOURCES_TASK_NAME, ResourcesTask
from tasks.tests.common import AsyncMockClient, TaskTestCase, UnexpectedException, async_generator, mock
from tasks.version import VERSION

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
included_metadata = ResourceMetadata(include=True)


class TestResourcesTask(TaskTestCase):
    TASK_NAME = RESOURCES_TASK_NAME

    def setUp(self) -> None:
        super().setUp()
        self.sub_client = AsyncMockClient()
        self.patch("SubscriptionClient").return_value = self.sub_client
        self.datadog_client = AsyncMockClient()
        self.patch_path("tasks.task.DatadogClient").return_value = self.datadog_client
        self.resource_client = self.patch("ResourceClient")
        self.resource_client_mapping: ResourceCache = {}
        self.log = self.patch_path("tasks.task.log").getChild.return_value

        self.resource_mock_client = AsyncMockClient()
        self.resource_mock_client.log = self.log

        def create_resource_client(_log: Any, _cred: Any, _tags: Any, sub_id: str):
            assert sub_id in self.resource_client_mapping, "subscription not mocked properly"
            self.resource_mock_client.get_resources_per_region.return_value = self.resource_client_mapping[sub_id]
            return self.resource_mock_client

        self.resource_client.side_effect = create_resource_client

    async def run_resources_task(
        self, cache: ResourceCache | ResourceCacheV1, execution_id: str = "", is_initial_run: bool = False
    ) -> ResourcesTask:
        async with ResourcesTask(
            dumps(cache, default=list), is_initial_run=is_initial_run, execution_id=execution_id
        ) as task:
            await task.run()
        return task

    @property
    def cache(self) -> ResourceCache:
        return self.cache_value(RESOURCE_CACHE_BLOB, _deserialize_v2_resource_cache)

    async def test_invalid_cache(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: {
                SUPPORTED_REGION_1: {
                    "res1": included_metadata,
                    "res2": included_metadata,
                }
            },
            sub_id2: {SUPPORTED_REGION_2: {"res3": included_metadata}},
        }

        async with ResourcesTask("[[[[{{{{{asjdklahjs]]]}}}") as task:
            await task.run()

        self.log.warning.assert_called_once_with("Resource Cache is in an invalid format, task will reset the cache")
        self.write_cache.assert_called_once()
        self.assertEqual(
            self.cache,
            {
                sub_id1: {
                    SUPPORTED_REGION_1: {
                        "res1": included_metadata,
                        "res2": included_metadata,
                    }
                },
                sub_id2: {SUPPORTED_REGION_2: {"res3": included_metadata}},
            },
        )

    async def test_cache_upgrade_schema(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: {SUPPORTED_REGION_1: {"res1": included_metadata, "res2": included_metadata}},
            sub_id2: {SUPPORTED_REGION_2: {"res3": included_metadata}},
        }

        await self.run_resources_task(
            {sub_id1: {SUPPORTED_REGION_1: {"res1", "res2"}}, sub_id2: {SUPPORTED_REGION_2: {"res3"}}}
        )

        self.log.warning.assert_called_once_with("Detected resource cache schema upgrade, flushing cache")
        self.write_cache.assert_called_once()

        self.assertEqual(
            self.cache,
            {
                sub_id1: {SUPPORTED_REGION_1: {"res1": included_metadata, "res2": included_metadata}},
                sub_id2: {SUPPORTED_REGION_2: {"res3": included_metadata}},
            },
        )

    async def test_cache_upgrade_schema_add_resources(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: {SUPPORTED_REGION_1: {"res1": included_metadata, "res2": included_metadata}},
            sub_id2: {SUPPORTED_REGION_2: {"res3": included_metadata}},
        }

        await self.run_resources_task(
            {
                sub_id1: {SUPPORTED_REGION_1: {"res1", "res2", "res3"}},
                sub_id2: {SUPPORTED_REGION_2: {"res4", "res5", "res6"}},
            }
        )

        self.log.warning.assert_called_once_with("Detected resource cache schema upgrade, flushing cache")
        self.assertEqual(self.write_cache.call_count, 2)

        self.assertEqual(
            self.cache,
            {
                sub_id1: {SUPPORTED_REGION_1: {"res1": included_metadata, "res2": included_metadata}},
                sub_id2: {SUPPORTED_REGION_2: {"res3": included_metadata}},
            },
        )

    async def test_empty_cache_adds_resources(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: {SUPPORTED_REGION_1: {"res1": included_metadata, "res2": included_metadata}},
            sub_id2: {SUPPORTED_REGION_2: {"res3": included_metadata}},
        }

        async with ResourcesTask("") as task:
            await task.run()

        self.log.warning.assert_called_once_with("Resource Cache is in an invalid format, task will reset the cache")
        self.write_cache.assert_called_once()
        self.assertEqual(
            self.cache,
            {
                sub_id1: {SUPPORTED_REGION_1: {"res1": included_metadata, "res2": included_metadata}},
                sub_id2: {SUPPORTED_REGION_2: {"res3": included_metadata}},
            },
        )

    async def test_no_new_resources_doesnt_cache(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: {SUPPORTED_REGION_1: {"res1": included_metadata, "res2": included_metadata}},
            sub_id2: {SUPPORTED_REGION_2: {"res3": included_metadata}},
        }
        await self.run_resources_task(
            {
                sub_id1: {SUPPORTED_REGION_1: {"res1": included_metadata, "res2": included_metadata}},
                sub_id2: {SUPPORTED_REGION_2: {"res3": included_metadata}},
            }
        )

        self.write_cache.assert_not_called()

    async def test_resources_gone(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: {},
            sub_id2: {},
        }
        await self.run_resources_task(
            {
                sub_id1: {SUPPORTED_REGION_2: {"res1": included_metadata, "res2": included_metadata}},
                sub_id2: {SUPPORTED_REGION_1: {"res3": included_metadata}},
            }
        )
        self.write_cache.assert_called_once()
        self.assertEqual(self.cache, {})

    async def test_subscriptions_gone(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator())
        # we dont return any subscriptions, so we should never call the resource client, if we do, it will error
        await self.run_resources_task(
            {
                sub_id1: {SUPPORTED_REGION_1: {"res1": included_metadata, "res2": included_metadata}},
                sub_id2: {SUPPORTED_REGION_2: {"res3": included_metadata}},
            }
        )
        self.write_cache.assert_called_once()
        self.assertEqual(self.cache, {})

    async def test_unexpected_failure_skips_cache_write(self):
        write_caches = self.patch("ResourcesTask.write_caches")
        self.sub_client.subscriptions.list = Mock(side_effect=UnexpectedException("unexpected"))
        with self.assertRaises(UnexpectedException):
            await self.run_resources_task(
                {
                    sub_id1: {SUPPORTED_REGION_1: {"res1": included_metadata, "res2": included_metadata}},
                    sub_id2: {SUPPORTED_REGION_2: {"res3": included_metadata}},
                }
            )
        write_caches.assert_not_awaited()

    async def test_unmonitored_subscriptions_ignored(self):
        sub_id3 = "6522f787-edd0-4005-a901-d61c0ee60cb8"

        def getenv_mock(name, default=None):
            env_vars = {
                "MONITORED_SUBSCRIPTIONS": '["a062baee-fdd3-4784-beb4-d817f591422c", "77602a31-36b2-4417-a27c-9071107ca3e6"]'
            }
            return env_vars.get(name, default)

        self.patch("getenv").side_effect = getenv_mock
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, Mock(subscription_id=sub_id3)))
        self.resource_client_mapping = {
            sub_id1: {SUPPORTED_REGION_1: {"res1": included_metadata, "res2": included_metadata}},
            sub_id2: {SUPPORTED_REGION_2: {"res3": included_metadata}},
        }
        await self.run_resources_task({})
        self.write_cache.assert_called_once()
        self.assertEqual(
            self.cache,
            {sub_id1: {SUPPORTED_REGION_1: {"res1": included_metadata, "res2": included_metadata}}},
        )

    async def test_tags(self):
        self.env[CONTROL_PLANE_ID_SETTING] = "a2b4c5d6"
        self.sub_client.subscriptions.list = Mock(return_value=async_generator())

        task = await self.run_resources_task({})

        self.assertCountEqual(
            task.tags,
            [
                "forwarder:lfocontrolplane",
                "task:resources_task",
                "control_plane_id:a2b4c5d6",
                f"version:{VERSION}",
            ],
        )

    async def test_initial_run_golden_path(self):
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: {SUPPORTED_REGION_1: {"res1": included_metadata, "res2": included_metadata}},
            sub_id2: {SUPPORTED_REGION_2: {"res3": included_metadata}},
        }
        status_client = AsyncMockClient()
        self.datadog_client.submit_status_update = status_client

        uuid = str(uuid4())

        await self.run_resources_task({}, is_initial_run=True, execution_id=uuid)

        expected_calls = [
            call("resources_task.task_start", StatusCode.OK, "Resources task started", uuid, "unknown", "unknown"),
            call("resources_task.task_complete", StatusCode.OK, "Resources task completed", uuid, "unknown", "unknown"),
        ]

        status_client.assert_has_calls(expected_calls)
        self.assertEqual(status_client.call_count, len(expected_calls))

    async def test_subscriptions_list_errors(self):
        test_string = "meow"
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(HttpResponseError(test_string)))
        self.resource_client_mapping = {
            sub_id1: {SUPPORTED_REGION_1: {"res1": included_metadata, "res2": included_metadata}},
            sub_id2: {SUPPORTED_REGION_2: {"res3": included_metadata}},
        }
        status_client = AsyncMockClient()
        self.datadog_client.submit_status_update = status_client

        uuid = str(uuid4())

        await self.run_resources_task({}, is_initial_run=True, execution_id=uuid)

        expected_calls = [
            call("resources_task.task_start", StatusCode.OK, "Resources task started", uuid, "unknown", "unknown"),
            call(
                "resources_task.subscriptions_list",
                StatusCode.AZURE_RESPONSE_ERROR,
                f"Failed to list subscriptions. Reason: {test_string}",
                uuid,
                "unknown",
                "unknown",
            ),
        ]

        status_client.assert_has_calls(expected_calls)
        self.assertEqual(status_client.call_count, len(expected_calls))

    async def test_resources_list_errors(self):
        test_string = "meow"
        self.sub_client.subscriptions.list = Mock(return_value=async_generator(sub1, sub2))
        self.resource_client_mapping = {
            sub_id1: {SUPPORTED_REGION_1: {"res1": included_metadata, "res2": included_metadata}},
            sub_id2: {SUPPORTED_REGION_2: {"res3": included_metadata}},
        }

        def get_resources_per_region(*args, **kwargs):
            raise HttpResponseError(test_string)

        self.resource_mock_client.get_resources_per_region = get_resources_per_region
        status_client = AsyncMockClient()
        self.datadog_client.submit_status_update = status_client

        uuid = str(uuid4())

        await self.run_resources_task({}, is_initial_run=True, execution_id=uuid)

        expected_calls = [
            call("resources_task.task_start", StatusCode.OK, "Resources task started", uuid, "unknown", "unknown"),
            call(
                "resources_task.resources_list",
                StatusCode.AZURE_RESPONSE_ERROR,
                f"Failed to list resources for subscription {sub_id1}. Reason: {test_string}",
                uuid,
                "unknown",
                "unknown",
            ),
            call(
                "resources_task.resources_list",
                StatusCode.AZURE_RESPONSE_ERROR,
                f"Failed to list resources for subscription {sub_id2}. Reason: {test_string}",
                uuid,
                "unknown",
                "unknown",
            ),
            call("resources_task.task_complete", StatusCode.OK, "Resources task completed", uuid, "unknown", "unknown"),
        ]

        status_client.assert_has_calls(expected_calls)
        self.assertEqual(status_client.call_count, len(expected_calls))
