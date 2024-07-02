from json import JSONDecodeError, loads
from typing import Any, TypeAlias

from jsonschema import ValidationError, validate

from cache.common import DIAGNOSTIC_SETTING_CONFIGURATION_SCHEMA, DiagnosticSettingConfiguration


DIAGNOSTIC_SETTINGS_CACHE_BLOB = "settings.json"

DiagnosticSettingsCache: TypeAlias = dict[str, dict[str, DiagnosticSettingConfiguration]]
"Mapping of subscription_id to resource_id to DiagnosticSettingConfiguration"


DIAGNOSTIC_SETTINGS_CACHE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "propertyNames": {"format": "uuid"},  # subscription_id
    "additionalProperties": {
        "type": "object",  # resource_id
        "additionalProperties": DIAGNOSTIC_SETTING_CONFIGURATION_SCHEMA,
    },
}


def deserialize_diagnostic_settings_cache(cache_str: str) -> tuple[bool, DiagnosticSettingsCache]:
    try:
        cache = loads(cache_str)
        validate(instance=cache, schema=DIAGNOSTIC_SETTINGS_CACHE_SCHEMA)
        return True, cache
    except (JSONDecodeError, ValidationError):
        return False, {}
