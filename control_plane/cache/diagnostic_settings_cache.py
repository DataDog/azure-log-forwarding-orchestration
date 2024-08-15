# stdlib
from typing import Any, TypeAlias

# 3p
from cache.common import deserialize_cache

DIAGNOSTIC_SETTINGS_CACHE_BLOB = "settings.json"


DiagnosticSettingsCache: TypeAlias = dict[str, dict[str, str]]
"Mapping of subscription_id to resource_id to DiagnosticSettingConfiguration"


DIAGNOSTIC_SETTINGS_CACHE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "propertyNames": {"format": "uuid"},  # subscription_id
    "additionalProperties": {
        "type": "object",  # resource_id
        "additionalProperties": {
            "type": "string",  # config id
        },
    },
}


def deserialize_diagnostic_settings_cache(cache_str: str) -> DiagnosticSettingsCache | None:
    """Deserialize the diagnostic settings cache. Returns None if the cache is invalid."""
    return deserialize_cache(cache_str, DIAGNOSTIC_SETTINGS_CACHE_SCHEMA)
