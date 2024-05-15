# stdlib
from abc import ABC, abstractmethod
from os import getenv
from typing import AsyncContextManager, Self
from logging import ERROR, getLogger

# 3p
from azure.core.exceptions import ResourceNotFoundError
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob.aio import BlobClient

# silence azure logging except for errors
getLogger("azure").setLevel(ERROR)

BLOB_STORAGE_CACHE = "resources-cache"
STORAGE_CONNECTION_SETTING = "AzureWebJobsStorage"


def get_env(name: str) -> str:
    """Get an environment variable or raise an error if not present"""
    if value := getenv(name):
        return value
    raise ValueError(f"Environment variable {name} not set")


CONNECTION_STRING = get_env(STORAGE_CONNECTION_SETTING)


async def read_cache(blob_name: str) -> str:
    async with BlobClient.from_connection_string(CONNECTION_STRING, BLOB_STORAGE_CACHE, blob_name) as blob_client:
        try:
            blob = await blob_client.download_blob()
        except ResourceNotFoundError:
            return ""
        return (await blob.readall()).decode()


async def write_cache(blob_name: str, content: str) -> None:
    async with BlobClient.from_connection_string(CONNECTION_STRING, BLOB_STORAGE_CACHE, blob_name) as blob_client:
        await blob_client.upload_blob(content, overwrite=True)


class Task(AsyncContextManager, ABC):
    def __init__(self) -> None:
        self.credential = DefaultAzureCredential()

    @abstractmethod
    async def run(self) -> None: ...

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        await self.write_caches()
        await self.credential.close()

    @abstractmethod
    async def write_caches(self) -> None: ...
