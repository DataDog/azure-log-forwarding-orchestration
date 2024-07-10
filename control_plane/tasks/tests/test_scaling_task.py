from json import dumps
from os import environ
from unittest.mock import AsyncMock, Mock
from cache.assignment_cache import ASSIGNMENT_CACHE_BLOB, AssignmentCache, deserialize_assignment_cache
from cache.common import DiagnosticSettingConfiguration, InvalidCacheError
from cache.resources_cache import ResourceCache
from tasks.scaling_task import SCALING_TASK_NAME, LogForwarderClient, ScalingTask
from tasks.tests.common import AsyncTestCase, TaskTestCase


EAST_US = "eastus"
WEST_US = "westus"


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

    async def test_create_log_forwarder(self):
        async with self.client:
            await self.client.create_log_forwarder(EAST_US)

        # app service plan
        self.client.web_client.app_service_plans.begin_create_or_update.assert_called_once()
        # storage account
        self.client.storage_client.storage_accounts.begin_create.assert_called_once()
        # function app
        self.client.web_client.web_apps.begin_create_or_update.assert_called_once()
        # deploy code
        self.client.rest_client.post.assert_called_once()

    async def test_create_log_forwarder_no_keys(self):
        self.client.storage_client.storage_accounts.list_keys = AsyncMock(return_value=Mock(keys=[]))
        with self.assertRaises(ValueError) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US)
        self.assertIn("No keys found for storage account", str(ctx.exception))

    async def test_create_log_forwarder_deploying_failure(self):
        (await self.client.rest_client.post()).raise_for_status = Mock(side_effect=Exception("400: Deploying failed"))
        with self.assertRaises(Exception) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US)
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

        self.log = self.patch("log")
        self.patch_path("tasks.scaling_task.uuid4", return_value=NEW_UUID)

    async def run_scaling_task(
        self, resource_cache_state: ResourceCache, assignment_cache_state: AssignmentCache, resource_group: str
    ) -> AssignmentCache:
        async with ScalingTask(
            dumps(resource_cache_state, default=list), dumps(assignment_cache_state), resource_group
        ) as task:
            await task.run()

        success, cache = deserialize_assignment_cache(self.cache_value(ASSIGNMENT_CACHE_BLOB))
        if not success:
            raise InvalidCacheError("Assignment Cache is in an invalid format after the task")
        return cache

    async def test_new_regions_are_added(self):
        configuration: DiagnosticSettingConfiguration = {
            "type": "storageaccount",
            "id": NEW_LOG_FORWARDER_ID,
            "storage_account_id": "some/storage/account",
        }
        self.client.create_log_forwarder.return_value = configuration

        cache = await self.run_scaling_task(
            resource_cache_state={sub_id1: {EAST_US: {"resource1", "resource2"}}},
            assignment_cache_state={},
            resource_group="test_lfo",
        )

        self.client.create_log_forwarder.assert_called_once_with(EAST_US)
        expected_cache: AssignmentCache = {
            sub_id1: {
                EAST_US: {
                    "resources": {"resource1": NEW_LOG_FORWARDER_ID, "resource2": NEW_LOG_FORWARDER_ID},
                    "configurations": {NEW_LOG_FORWARDER_ID: configuration},
                }
            }
        }
        self.assertEqual(cache, expected_cache)

    async def test_gone_regions_are_deleted(self):
        cache = await self.run_scaling_task(
            resource_cache_state={},
            assignment_cache_state={
                sub_id1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: {
                                "type": "storageaccount",
                                "id": OLD_LOG_FORWARDER_ID,
                                "storage_account_id": "some/storage/account",
                            },
                        },
                    }
                },
            },
            resource_group="test_lfo",
        )

        self.client.delete_log_forwarder.assert_awaited_once_with(EAST_US, OLD_LOG_FORWARDER_ID)

        self.assertEqual(cache, {sub_id1: {}})

    async def test_regions_added_and_deleted(self):
        configuration: DiagnosticSettingConfiguration = {
            "type": "storageaccount",
            "id": NEW_LOG_FORWARDER_ID,
            "storage_account_id": "some/other/storage/account",
        }
        self.client.create_log_forwarder.return_value = configuration

        cache = await self.run_scaling_task(
            resource_cache_state={sub_id1: {WEST_US: {"resource1", "resource2"}}},
            assignment_cache_state={
                sub_id1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: {
                                "type": "storageaccount",
                                "id": OLD_LOG_FORWARDER_ID,
                                "storage_account_id": "some/storage/account",
                            },
                        },
                    }
                },
            },
            resource_group="test_lfo",
        )

        self.client.create_log_forwarder.assert_called_once_with(WEST_US)
        self.client.delete_log_forwarder.assert_called_once_with(EAST_US, "5a095f74c60a")

        expected_cache: AssignmentCache = {
            sub_id1: {
                WEST_US: {
                    "resources": {"resource1": NEW_LOG_FORWARDER_ID, "resource2": NEW_LOG_FORWARDER_ID},
                    "configurations": {NEW_LOG_FORWARDER_ID: configuration},
                }
            }
        }
        self.assertEqual(cache, expected_cache)
