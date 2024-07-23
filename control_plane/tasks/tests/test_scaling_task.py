from json import dumps
from os import environ
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import UUID
from cache.assignment_cache import ASSIGNMENT_CACHE_BLOB, AssignmentCache, deserialize_assignment_cache
from cache.common import STORAGE_ACCOUNT_TYPE, InvalidCacheError
from cache.resources_cache import ResourceCache
from tasks.scaling_task import SCALING_TASK_NAME, LogForwarderClient, ScalingTask
from tasks.tests.common import AsyncTestCase, TaskTestCase


EAST_US = "eastus"
WEST_US = "westus"
LOG_FORWARDER_ID = "d6fc2c757f9c"


class TestLogForwarderClient(AsyncTestCase):
    async def asyncSetUp(self) -> None:
        environ["AzureWebJobsStorage"] = "..."
        self.client: AsyncMock = LogForwarderClient(  # type: ignore
            credential=AsyncMock(), subscription_id="sub_id", resource_group="test_lfo"
        )
        await self.client.__aexit__(None, None, None)
        self.client.rest_client = AsyncMock()
        self.client.web_client = AsyncMock()
        self.client.storage_client = AsyncMock()
        self.client.storage_client.storage_accounts.list_keys = AsyncMock(return_value=Mock(keys=[Mock(value="key")]))
        self.container_client = self.patch_path("tasks.scaling_task.ContainerClient")

        self.raise_for_status = Mock()
        (await self.client.rest_client.post()).raise_for_status = self.raise_for_status
        self.client.rest_client.post.reset_mock()

    async def test_create_log_forwarder(self):
        async with self.client:
            await self.client.create_log_forwarder(EAST_US, LOG_FORWARDER_ID)

        # app service plan
        asp_create: AsyncMock = self.client.web_client.app_service_plans.begin_create_or_update
        asp_create.assert_awaited_once()
        (await asp_create()).result.assert_awaited_once()
        # storage account
        storage_create: AsyncMock = self.client.storage_client.storage_accounts.begin_create
        (await storage_create()).result.assert_awaited_once()
        # function app
        function_create: AsyncMock = self.client.web_client.web_apps.begin_create_or_update
        (await function_create()).result.assert_awaited_once()
        # deploy code
        self.client.rest_client.post.assert_awaited_once()

    async def test_create_log_forwarder_no_keys(self):
        self.client.storage_client.storage_accounts.list_keys = AsyncMock(return_value=Mock(keys=[]))
        with self.assertRaises(ValueError) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US, LOG_FORWARDER_ID)
        self.assertIn("No keys found for storage account", str(ctx.exception))

    async def test_create_log_forwarder_app_service_plan_failure(self):
        (await self.client.web_client.app_service_plans.begin_create_or_update()).result.side_effect = Exception(
            "400: ASP creation failed"
        )
        with self.assertRaises(Exception) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US, LOG_FORWARDER_ID)
        self.assertIn("400: ASP creation failed", str(ctx.exception))

    async def test_create_log_forwarder_storage_account_failure(self):
        (await self.client.storage_client.storage_accounts.begin_create()).result.side_effect = Exception(
            "400: Storage Account creation failed"
        )
        with self.assertRaises(Exception) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US, LOG_FORWARDER_ID)
        self.assertIn("400: Storage Account creation failed", str(ctx.exception))

    async def test_create_log_forwarder_function_app_failure(self):
        (await self.client.web_client.web_apps.begin_create_or_update()).result.side_effect = Exception(
            "400: Function App creation failed"
        )
        with self.assertRaises(Exception) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US, LOG_FORWARDER_ID)
        self.assertIn("400: Function App creation failed", str(ctx.exception))

    async def test_create_log_forwarder_deploying_failure(self):
        self.raise_for_status.side_effect = Exception("400: Deploying failed")
        with self.assertRaises(Exception) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US, LOG_FORWARDER_ID)
        self.assertIn("400: Deploying failed", str(ctx.exception))


sub_id1 = "decc348e-ca9e-4925-b351-ae56b0d9f811"

NEW_UUID = "04cb0e0b-f268-4349-aa32-93a5885365f5"
OLD_LOG_FORWARDER_ID = "5a095f74c60a"
NEW_LOG_FORWARDER_ID = "93a5885365f5"


class TestScalingTask(TaskTestCase):
    TASK_NAME = SCALING_TASK_NAME

    async def asyncSetUp(self) -> None:
        super().setUp()
        client = AsyncMock()
        self.patch_path("tasks.scaling_task.LogForwarderClient").return_value = client
        self.client = await client.__aenter__()
        self.client.create_log_forwarder.return_value = STORAGE_ACCOUNT_TYPE

        self.log = self.patch("log")
        self.uuid = self.patch("uuid4")
        self.uuid.return_value = UUID(NEW_UUID)

    @property
    def cache(self):
        success, cache = deserialize_assignment_cache(self.cache_value(ASSIGNMENT_CACHE_BLOB))
        self.assertTrue(success)
        return cache

    async def run_scaling_task(
        self, resource_cache_state: ResourceCache, assignment_cache_state: AssignmentCache, resource_group: str
    ):
        async with ScalingTask(
            dumps(resource_cache_state, default=list), dumps(assignment_cache_state), resource_group
        ) as task:
            await task.run()

    async def test_scaling_task_fails_without_valid_resource_cache(self):
        with self.assertRaises(InvalidCacheError):
            ScalingTask("invalid json", "{}", "lfo")

    async def test_reset_invalid_scaling_cache(self):
        invalid_cache: Any = "not valid"
        await self.run_scaling_task(
            {},
            invalid_cache,
            "lfo",
        )
        self.write_cache.assert_not_awaited()
        self.log.warning.assert_called_once_with("Assignment Cache is in an invalid format, task will reset the cache")

    async def test_new_regions_are_added(self):
        await self.run_scaling_task(
            resource_cache_state={sub_id1: {EAST_US: {"resource1", "resource2"}}},
            assignment_cache_state={},
            resource_group="test_lfo",
        )

        self.client.create_log_forwarder.assert_called_once_with(EAST_US, NEW_LOG_FORWARDER_ID)
        expected_cache: AssignmentCache = {
            sub_id1: {
                EAST_US: {
                    "resources": {"resource1": NEW_LOG_FORWARDER_ID, "resource2": NEW_LOG_FORWARDER_ID},
                    "configurations": {NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                }
            }
        }
        self.assertEqual(self.cache, expected_cache)

    async def test_gone_regions_are_deleted(self):
        await self.run_scaling_task(
            resource_cache_state={},
            assignment_cache_state={
                sub_id1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                },
            },
            resource_group="test_lfo",
        )

        self.client.delete_log_forwarder.assert_awaited_once_with(EAST_US, OLD_LOG_FORWARDER_ID)

        self.assertEqual(self.cache, {sub_id1: {}})

    async def test_regions_added_and_deleted(self):
        await self.run_scaling_task(
            resource_cache_state={sub_id1: {WEST_US: {"resource1", "resource2"}}},
            assignment_cache_state={
                sub_id1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                },
            },
            resource_group="test_lfo",
        )

        self.client.create_log_forwarder.assert_called_once_with(WEST_US, NEW_LOG_FORWARDER_ID)
        self.client.delete_log_forwarder.assert_called_once_with(EAST_US, "5a095f74c60a")

        expected_cache: AssignmentCache = {
            sub_id1: {
                WEST_US: {
                    "resources": {"resource1": NEW_LOG_FORWARDER_ID, "resource2": NEW_LOG_FORWARDER_ID},
                    "configurations": {NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                }
            }
        }
        self.assertEqual(self.cache, expected_cache)
