from os import environ
from typing import Any, Literal, TypeAlias, TypedDict

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob.aio import BlobClient


BLOB_STORAGE_CACHE = "resources-cache"

STORAGE_CONNECTION_SETTING = "AzureWebJobsStorage"
CONNECTION_STRING = environ[STORAGE_CONNECTION_SETTING]


EVENT_HUB_DIAGNOSTIC_SETTING_TYPE = "eventhub"
STORAGE_ACCOUNT_DIAGNOSTIC_SETTING_TYPE = "storageaccount"


class EventHubDiagnosticSettingConfiguration(TypedDict, total=True):
    id: str
    type: Literal["eventhub"]
    event_hub_name: str
    event_hub_namespace: str


EVENT_HUB_DIAGNOSTIC_SETTING_CONFIGURATION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "type": {
            "type": "string",
            "const": EVENT_HUB_DIAGNOSTIC_SETTING_TYPE,
        },
        "event_hub_name": {"type": "string"},
        "event_hub_namespace": {"type": "string"},
    },
    "required": ["id", "event_hub_name", "event_hub_namespace"],
    "additionalProperties": False,
}


class StorageAccountDiagnosticSettingConfiguration(TypedDict, total=True):
    id: str
    type: Literal["storageaccount"]
    storage_account_id: str


STORAGE_ACCOUNT_DIAGNOSTIC_SETTING_CONFIGURATION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "type": {
            "type": "string",
            "const": STORAGE_ACCOUNT_DIAGNOSTIC_SETTING_TYPE,
        },
        "storage_account_id": {"type": "string"},
    },
    "required": ["id", "storage_account_id"],
    "additionalProperties": False,
}

DiagnosticSettingConfiguration: TypeAlias = (
    EventHubDiagnosticSettingConfiguration | StorageAccountDiagnosticSettingConfiguration
)

DIAGNOSTIC_SETTING_CONFIGURATION_SCHEMA: dict[str, Any] = {
    "oneOf": [
        EVENT_HUB_DIAGNOSTIC_SETTING_CONFIGURATION_SCHEMA,
        STORAGE_ACCOUNT_DIAGNOSTIC_SETTING_CONFIGURATION_SCHEMA,
    ],
}


class InvalidCacheError(Exception):
    pass


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
