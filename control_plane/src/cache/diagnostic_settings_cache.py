from json import JSONDecodeError, loads
from typing import TypeAlias, TypedDict

from jsonschema import ValidationError, validate


DIAGNOSTIC_SETTINGS_CACHE_BLOB = "settings.json"


class DiagnosticSettingConfiguration(TypedDict):
    id: str
    event_hub_name: str
    event_hub_namespace: str


DiagnosticSettingsCache: TypeAlias = dict[str, dict[str, DiagnosticSettingConfiguration]]
"Mapping of subscription_id to resource_id to DiagnosticSettingConfiguration"

DIAGNOSTIC_SETTINGS_CACHE_SCHEMA = {
    "type": "object",
    "propertyNames": {"format": "uuid"},
    "additionalProperties": {
        "type": "object",
        "patternProperties": {
            ".*": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "event_hub_name": {"type": "string"},
                    "event_hub_namespace": {"type": "string"},
                },
                "required": ["id", "event_hub_name", "event_hub_namespace"],
                "additionalProperties": False,
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
