# stdlib
from os import environ
from typing import Any, Literal

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


EVENT_HUB_DIAGNOSTIC_SETTING_TYPE = "eventhub"
STORAGE_ACCOUNT_DIAGNOSTIC_SETTING_TYPE = "storageaccount"

FUNCTION_APP_PREFIX = "dd-blob-log-forwarder-"
ASP_PREFIX = "dd-log-forwarder-plan-"
STORAGE_ACCOUNT_PREFIX = "ddlogstorage"


def get_function_app_name(config_id: str) -> str:
    return FUNCTION_APP_PREFIX + config_id


def get_resource_group_id(subscription_id: str, resource_group: str) -> str:
    return f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"


def get_function_app_id(subscription_id: str, resource_group: str, config_id: str) -> str:
    return (
        get_resource_group_id(subscription_id, resource_group)
        + "/providers/Microsoft.Web/sites/"
        + get_function_app_name(config_id)
    )


def get_app_service_plan_name(config_id: str) -> str:
    return ASP_PREFIX + config_id


def get_app_service_plan_id(subscription_id: str, resource_group: str, config_id: str) -> str:
    return (
        get_resource_group_id(subscription_id, resource_group)
        + "/providers/Microsoft.Web/serverfarms/"
        + get_app_service_plan_name(config_id)
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
# EVENT_HUB_NAME_PREFIX = ...
# EVENT_HUB_NAMESPACE_PREFIX = ...

DiagnosticSettingType = Literal["eventhub", "storageaccount"]

DIAGNOSTIC_SETTING_TYPE_SCHEMA: dict[str, Any] = {
    "oneOf": [
        {"const": STORAGE_ACCOUNT_DIAGNOSTIC_SETTING_TYPE},
        {"const": EVENT_HUB_DIAGNOSTIC_SETTING_TYPE},
    ],
}


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
