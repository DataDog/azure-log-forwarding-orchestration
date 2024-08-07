# stdlib
from os import environ
from unittest.mock import AsyncMock, MagicMock, Mock

# 3p
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from tenacity import RetryError

# project
from cache.common import FUNCTION_APP_PREFIX, STORAGE_ACCOUNT_PREFIX
from tasks.client.log_forwarder_client import LogForwarderClient
from tasks.tests.common import AsyncTestCase

sub_id1 = "decc348e-ca9e-4925-b351-ae56b0d9f811"
EAST_US = "eastus"
WEST_US = "westus"
log_forwarder_id = "d6fc2c757f9c"
log_forwarder_name = FUNCTION_APP_PREFIX + log_forwarder_id
storage_account_name = STORAGE_ACCOUNT_PREFIX + log_forwarder_id
rg1 = "test_lfo"


class AsyncContextManagerMock(MagicMock):
    async def __aenter__(self):
        return self.aenter

    async def __aexit__(self, *args):
        pass


class FakeHttpError(HttpResponseError):
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def __eq__(self, value: object) -> bool:
        return isinstance(value, FakeHttpError) and value.status_code == self.status_code


class MockedLogForwarderClient(LogForwarderClient):
    """Used for typing since we know the underlying clients will be mocks"""

    rest_client: AsyncMock
    web_client: AsyncMock
    storage_client: AsyncMock
    monitor_client: AsyncMock
    api_client: AsyncMock


class TestLogForwarderClient(AsyncTestCase):
    async def asyncSetUp(self) -> None:
        environ["AzureWebJobsStorage"] = "..."
        self.client: MockedLogForwarderClient = LogForwarderClient(  # type: ignore
            credential=AsyncMock(), subscription_id=sub_id1, resource_group=rg1
        )
        await self.client.__aexit__(None, None, None)
        self.client.rest_client = AsyncMock()
        self.client.web_client = AsyncMock()
        self.client.storage_client = AsyncMock()
        self.client.monitor_client = AsyncMock()
        self.client.api_client = AsyncMock()
        self.client.storage_client.storage_accounts.list_keys = AsyncMock(return_value=Mock(keys=[Mock(value="key")]))
        self.container_client = self.patch_path(
            "tasks.client.log_forwarder_client.ContainerClient", new_callable=AsyncContextManagerMock
        )

        self.raise_for_status = Mock()
        (await self.client.rest_client.post()).raise_for_status = self.raise_for_status
        self.client.rest_client.post.reset_mock()

    async def test_create_log_forwarder(self):
        async with self.client:
            await self.client.create_log_forwarder(EAST_US, log_forwarder_id)

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
                await self.client.create_log_forwarder(EAST_US, log_forwarder_id)
        self.assertIn("No keys found for storage account", str(ctx.exception))

    async def test_create_log_forwarder_app_service_plan_failure(self):
        (await self.client.web_client.app_service_plans.begin_create_or_update()).result.side_effect = Exception(
            "400: ASP creation failed"
        )
        with self.assertRaises(Exception) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US, log_forwarder_id)
        self.assertIn("400: ASP creation failed", str(ctx.exception))

    async def test_create_log_forwarder_storage_account_failure(self):
        (await self.client.storage_client.storage_accounts.begin_create()).result.side_effect = Exception(
            "400: Storage Account creation failed"
        )
        with self.assertRaises(Exception) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US, log_forwarder_id)
        self.assertIn("400: Storage Account creation failed", str(ctx.exception))

    async def test_create_log_forwarder_function_app_failure(self):
        (await self.client.web_client.web_apps.begin_create_or_update()).result.side_effect = Exception(
            "400: Function App creation failed"
        )
        with self.assertRaises(Exception) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US, log_forwarder_id)
        self.assertIn("400: Function App creation failed", str(ctx.exception))

    async def test_create_log_forwarder_deploying_failure(self):
        self.raise_for_status.side_effect = Exception("400: Deploying failed")
        with self.assertRaises(Exception) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US, log_forwarder_id)
        self.assertIn("400: Deploying failed", str(ctx.exception))

    async def test_delete_log_forwarder(self):
        async with self.client as client:
            success = await client.delete_log_forwarder(log_forwarder_id)
        self.assertTrue(success)
        self.client.web_client.web_apps.delete.assert_awaited_once_with(
            rg1, log_forwarder_name, delete_empty_server_farm=True
        )
        self.client.storage_client.storage_accounts.delete.assert_awaited_once_with(rg1, storage_account_name)

    async def test_delete_log_forwarder_ignore_resource_not_found(self):
        self.client.web_client.web_apps.delete.side_effect = ResourceNotFoundError()
        self.client.storage_client.storage_accounts.delete.side_effect = ResourceNotFoundError()
        async with self.client as client:
            success = await client.delete_log_forwarder(log_forwarder_id)
        self.assertTrue(success)
        self.client.web_client.web_apps.delete.assert_awaited_once_with(
            rg1, log_forwarder_name, delete_empty_server_farm=True
        )
        self.client.storage_client.storage_accounts.delete.assert_awaited_once_with(rg1, storage_account_name)

    async def test_delete_log_forwarder_not_raise_error(self):
        self.client.web_client.web_apps.delete.side_effect = FakeHttpError(400)
        async with self.client as client:
            success = await client.delete_log_forwarder(log_forwarder_id, raise_error=False)
        self.assertFalse(success)
        self.client.web_client.web_apps.delete.assert_awaited_once_with(
            rg1, log_forwarder_name, delete_empty_server_farm=True
        )
        self.client.storage_client.storage_accounts.delete.assert_awaited_once_with(rg1, storage_account_name)

    async def test_delete_log_forwarder_makes_3_retryable_attempts_default(self):
        self.client.web_client.web_apps.delete.side_effect = FakeHttpError(429)
        with self.assertRaises(RetryError) as ctx:
            async with self.client as client:
                await client.delete_log_forwarder(log_forwarder_id)
        self.assertEqual(ctx.exception.last_attempt.exception(), FakeHttpError(429))
        self.assertCalledTimesWith(
            self.client.web_client.web_apps.delete, 3, rg1, log_forwarder_name, delete_empty_server_farm=True
        )
        self.assertCalledTimesWith(self.client.storage_client.storage_accounts.delete, 3, rg1, storage_account_name)

    async def test_delete_log_forwarder_makes_5_retryable_attempts(self):
        self.client.web_client.web_apps.delete.side_effect = FakeHttpError(529)
        self.client.storage_client.storage_accounts.delete.side_effect = ResourceNotFoundError()
        with self.assertRaises(RetryError) as ctx:
            async with self.client as client:
                await client.delete_log_forwarder(log_forwarder_id, max_attempts=5)
        self.assertEqual(ctx.exception.last_attempt.exception(), FakeHttpError(529))
        self.assertCalledTimesWith(
            self.client.web_client.web_apps.delete, 5, rg1, log_forwarder_name, delete_empty_server_farm=True
        )
        self.assertCalledTimesWith(self.client.storage_client.storage_accounts.delete, 5, rg1, storage_account_name)

    async def test_delete_log_forwarder_doesnt_retry_after_second_unretryable(self):
        web_call_count = 0

        def web_side_effect(*args, **kwargs):
            nonlocal web_call_count
            web_call_count += 1
            if web_call_count < 2:
                raise FakeHttpError(429)
            raise FakeHttpError(400)

        self.client.web_client.web_apps.delete.side_effect = web_side_effect
        self.client.storage_client.storage_accounts.delete.side_effect = ResourceNotFoundError()

        with self.assertRaises(FakeHttpError) as ctx:
            async with self.client as client:
                await client.delete_log_forwarder(log_forwarder_id)
        self.assertEqual(ctx.exception, FakeHttpError(400))
        self.assertCalledTimesWith(
            self.client.web_client.web_apps.delete, 2, rg1, log_forwarder_name, delete_empty_server_farm=True
        )
        self.assertCalledTimesWith(self.client.storage_client.storage_accounts.delete, 2, rg1, storage_account_name)

    async def test_get_blob_metrics_standard_execution(self):
        self.container_client.from_connection_string = MagicMock(AsyncContextManagerMock)
        async with self.client as client:
            res = await client.get_blob_metrics("test", "test")
            self.assertEquals(res, ["", ""])
