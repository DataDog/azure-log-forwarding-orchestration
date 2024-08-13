# stdlib
from os import environ
from typing import Any, Final, Literal, NamedTuple

# 3p
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob.aio import BlobClient

BLOB_STORAGE_CACHE = "control-plane-cache"

STORAGE_CONNECTION_SETTING = "AzureWebJobsStorage"


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

CONTAINER_APP_PREFIX: Final = "dd-blob-log-forwarder-"
MANAGED_ENVIRONMENT_PREFIX: Final = "dd-log-forwarder-env-"
STORAGE_ACCOUNT_PREFIX: Final = "ddlogstorage"


def get_container_app_name(config_id: str) -> str:
    return CONTAINER_APP_PREFIX + config_id


def get_resource_group_id(subscription_id: str, resource_group: str) -> str:
    return f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"


def get_container_app_id(subscription_id: str, resource_group: str, config_id: str) -> str:
    return (
        get_resource_group_id(subscription_id, resource_group)
        + "/providers/Microsoft.App/jobs/"
        + get_container_app_name(config_id)
    )


def get_managed_env_name(config_id: str) -> str:
    return MANAGED_ENVIRONMENT_PREFIX + config_id


def get_managed_env_id(subscription_id: str, resource_group: str, config_id: str) -> str:
    return (
        get_resource_group_id(subscription_id, resource_group)
        + "/providers/Microsoft.App/managedEnvironments/"
        + get_managed_env_name(config_id)
    )


def get_storage_account_name(config_id: str) -> str:
    return STORAGE_ACCOUNT_PREFIX + config_id


def get_storage_account_id(subscription_id: str, resource_group: str, config_id: str) -> str:
    return (
        get_resource_group_id(subscription_id, resource_group)
        + "/providers/Microsoft.Storage/storageAccounts/"
        + get_storage_account_name(config_id)
    )


# TODO We will need to add prefixes for these when we implement event hub support
EVENT_HUB_NAME_PREFIX = NotImplemented
EVENT_HUB_NAMESPACE_PREFIX = NotImplemented


def get_event_hub_name(config_id: str) -> str:  # pragma: no cover
    return EVENT_HUB_NAME_PREFIX + config_id


def get_event_hub_namespace(config_id: str) -> str:  # pragma: no cover
    return EVENT_HUB_NAMESPACE_PREFIX + config_id


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
            blob = await blob_client.download_blob()
        except ResourceNotFoundError:
            return ""
        return (await blob.readall()).decode()


async def write_cache(blob_name: str, content: str) -> None:
    async with BlobClient.from_connection_string(
        get_config_option(STORAGE_CONNECTION_SETTING), BLOB_STORAGE_CACHE, blob_name
    ) as blob_client:
        await blob_client.upload_blob(content, overwrite=True)
