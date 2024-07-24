# stdlib
from os import environ
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, patch

# 3p
from azure.core.exceptions import ResourceNotFoundError

# project
from cache.common import (
    STORAGE_CONNECTION_SETTING,
    MissingConfigOptionError,
    get_app_service_plan_id,
    get_config_option,
    get_function_app_id,
    get_resource_group_id,
    get_storage_account_id,
    read_cache,
    write_cache,
)

sub1 = "sub1"
rg1 = "rg1"
config1 = "config1"


class TestCommon(TestCase):
    def test_missing_config_option(self):
        with self.assertRaises(MissingConfigOptionError) as ctx:
            get_config_option("missing_option")

        self.assertEqual(str(ctx.exception), "Missing required configuration option: missing_option")

    def test_get_resource_group_id(self):
        self.assertEqual(
            "/subscriptions/sub1/resourceGroups/rg1",
            get_resource_group_id("sub1", "rg1"),
        )

    def test_get_function_app_id(self):
        self.assertEqual(
            "/subscriptions/sub1/resourceGroups/rg1/providers/Microsoft.Web/sites/dd-blob-log-forwarder-config1",
            get_function_app_id(sub1, rg1, config1),
        )

    def test_get_app_service_plan_id(self):
        self.assertEqual(
            "/subscriptions/sub1/resourceGroups/rg1/providers/Microsoft.Web/serverfarms/dd-log-forwarder-plan-config1",
            get_app_service_plan_id(sub1, rg1, config1),
        )

    def test_get_storage_account_id(self):
        self.assertEqual(
            "/subscriptions/sub1/resourceGroups/rg1/providers/Microsoft.Storage/storageAccounts/ddlogstorageconfig1",
            get_storage_account_id(sub1, rg1, config1),
        )


class TestCacheUtils(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        client_patch = patch("cache.common.BlobClient")
        self.addCleanup(client_patch.stop)
        self.client_class = client_patch.start()

        environ[STORAGE_CONNECTION_SETTING] = "connection_string"

        self.client: AsyncMock = await self.client_class.from_connection_string.return_value.__aenter__()

    def tearDown(self) -> None:
        del environ[STORAGE_CONNECTION_SETTING]

    async def test_read_cache(self):
        blob = AsyncMock()
        self.client.download_blob = AsyncMock(return_value=blob)
        blob.readall = AsyncMock(return_value=b"test")

        cache_value = await read_cache("test.txt")
        self.assertEqual("test", cache_value)
        self.client_class.from_connection_string.assert_called_once_with(
            "connection_string", "control-plane-cache", "test.txt"
        )

    async def test_read_cache_no_blob(self):
        self.client.download_blob = AsyncMock(side_effect=ResourceNotFoundError)

        cache_value = await read_cache("test.txt")
        self.assertEqual("", cache_value)
        self.client_class.from_connection_string.assert_called_once_with(
            "connection_string", "control-plane-cache", "test.txt"
        )

    async def test_write_cache(self):
        await write_cache("test.txt", "test")

        self.client.upload_blob.assert_awaited_once_with("test", overwrite=True)
        self.client_class.from_connection_string.assert_called_once_with(
            "connection_string", "control-plane-cache", "test.txt"
        )
