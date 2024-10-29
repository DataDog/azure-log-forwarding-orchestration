# stdlib
from collections.abc import Callable
from json import JSONDecodeError, loads
from logging import getLogger
from os import environ
from typing import Any, Final, Literal, NamedTuple, TypeVar

# 3p
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob.aio import BlobClient, StorageStreamDownloader
from jsonschema import ValidationError, validate

BLOB_STORAGE_CACHE = "control-plane-cache"

STORAGE_CONNECTION_SETTING = "AzureWebJobsStorage"

log = getLogger(__name__)


class MissingConfigOptionError(Exception):
    def __init__(self, option: str) -> None:
        super().__init__(f"Missing required configuration option: {option}")


def get_config_option(name: str) -> str:
    """Get a configuration option from the environment or raise a helpful error"""
    if option := environ.get(name):
        return option
    raise MissingConfigOptionError(name)


EVENT_HUB_TYPE: Final = "eventhub"
STORAGE_ACCOUNT_TYPE: Final = "storageaccount"


LogForwarderType = Literal["eventhub", "storageaccount"]

LOG_FORWARDER_TYPE_SCHEMA: dict[str, Any] = {
    "oneOf": [
        {"const": STORAGE_ACCOUNT_TYPE},
        {"const": EVENT_HUB_TYPE},
    ],
}


class LogForwarder(NamedTuple):
    config_id: str
    type: LogForwarderType


class InvalidCacheError(Exception):
    pass


async def read_cache(blob_name: str) -> str:
    async with BlobClient.from_connection_string(
        get_config_option(STORAGE_CONNECTION_SETTING), BLOB_STORAGE_CACHE, blob_name
    ) as blob_client:
        try:
            blob: StorageStreamDownloader[bytes] = await blob_client.download_blob()
        except ResourceNotFoundError:
            return ""
        return (await blob.readall()).decode()


async def write_cache(blob_name: str, content: str) -> None:
    async with BlobClient.from_connection_string(
        get_config_option(STORAGE_CONNECTION_SETTING), BLOB_STORAGE_CACHE, blob_name
    ) as blob_client:
        await blob_client.upload_blob(content, overwrite=True)


T = TypeVar("T")


def deserialize_cache(
    cache_str: str, schema: dict[str, Any], post_processing: Callable[[T], T | None] = lambda x: x
) -> T | None:
    try:
        cache = loads(cache_str)
        validate(instance=cache, schema=schema)
        return post_processing(cache)
    except (JSONDecodeError, ValidationError, InvalidCacheError) as e:
        log.warning("Failed to deserialize cache: %s", e)
        return None
