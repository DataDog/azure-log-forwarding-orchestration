from json import JSONDecodeError, loads
from typing import Any, Literal, TypeAlias, TypedDict

from jsonschema import ValidationError, validate


DIAGNOSTIC_SETTINGS_CACHE_BLOB = "settings.json"

EVENT_HUB_DIAGNOSTIC_SETTING_TYPE = "eventhub"
STORAGE_ACCOUNT_DIAGNOSTIC_SETTING_TYPE = "storageaccount"


class EventHubDiagnosticSettingConfiguration(TypedDict):
    id: str
    type: Literal["eventhub"]
    event_hub_name: str
    event_hub_namespace: str


class StorageAccountDiagnosticSettingConfiguration(TypedDict):
    id: str
    type: Literal["storageaccount"]
    storage_account_id: str


DiagnosticSettingConfiguration: TypeAlias = (
    EventHubDiagnosticSettingConfiguration | StorageAccountDiagnosticSettingConfiguration
)

DiagnosticSettingsCache: TypeAlias = dict[str, dict[str, dict[str, DiagnosticSettingConfiguration]]]
"Mapping of subscription_id to region to resource_id to DiagnosticSettingConfiguration"

EVENT_HUB_DIAGNOSTIC_SETTING_CONFIGURATION: dict[str, Any] = {
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

STORAGE_ACCOUNT_DIAGNOSTIC_SETTING_CONFIGURATION: dict[str, Any] = {
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

DIAGNOSTIC_SETTINGS_CACHE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "propertyNames": {"format": "uuid"},  # subscription_id
    "additionalProperties": {
        "type": "object",  # region
        "additionalProperties": {
            "type": "object",  # resource_id
            "additionalProperties": {
                "oneOf": [
                    EVENT_HUB_DIAGNOSTIC_SETTING_CONFIGURATION,
                    STORAGE_ACCOUNT_DIAGNOSTIC_SETTING_CONFIGURATION,
                ],
            },
        },
    },
}


def deserialize_diagnostic_settings_cache(cache_str: str) -> tuple[bool, DiagnosticSettingsCache]:
    try:
        cache = loads(cache_str)
        validate(instance=cache, schema=DIAGNOSTIC_SETTINGS_CACHE_SCHEMA)
        return True, cache
    except (JSONDecodeError, ValidationError):
        return False, {}
