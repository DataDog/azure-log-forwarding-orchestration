# stdlib
from os import environ
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

# 3p
from azure.core.exceptions import ResourceNotFoundError
from tasks.tests.common import AsyncMockClient

# project
from cache.common import (
    read_cache,
    write_cache,
)
from cache.env import STORAGE_CONNECTION_SETTING

sub1 = "sub1"
rg1 = "rg1"
config1 = "config1"


class TestCacheUtils(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        client_patch = patch("cache.common.BlobClient")
        self.addCleanup(client_patch.stop)
        self.client_class = client_patch.start()

        environ[STORAGE_CONNECTION_SETTING] = "connection_string"
        self.client = AsyncMockClient()
        self.client_class.from_connection_string.return_value = self.client

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
