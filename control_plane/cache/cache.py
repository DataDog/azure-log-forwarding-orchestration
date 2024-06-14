from os import environ

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob.aio import BlobClient


BLOB_STORAGE_CACHE = "resources-cache"

STORAGE_CONNECTION_SETTING = "AzureWebJobsStorage"
CONNECTION_STRING = environ[STORAGE_CONNECTION_SETTING]


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
