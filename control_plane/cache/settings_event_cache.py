# stdlib
from typing import Any, TypeAlias

# 3p
from cache.common import deserialize_cache

EVENT_CACHE_BLOB = "settings_event.json"
DIAGNOSTIC_SETTINGS_COUNT = "diagnostic_settings_count"
SENT_EVENT = "sent_event"

DiagnosticSettingsEventCache: TypeAlias = dict[str, dict[str, dict[str, str | bool | int]]]
"""
ex) 
{
    "subscription_uuid1": {
        "resource_id1": {
            "diagnostic_settings_count": 5,
            "sent_event": true,
        },
        "resource_id2": { ... },
    },
    "subscription_uuid2": { .... }
}
"""


SETTINGS_EVENT_CACHE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "propertyNames": {"format": "uuid"},
    "additionalProperties": {
        "type": "object",
        "additionalProperties": {
            "type": "object",
            "additionalProperties": {"type": ["integer", "boolean"]},
        },
    },
}


def deserialize_event_cache(cache_str: str) -> DiagnosticSettingsEventCache | None:
    """Deserialize the diagnostic settings event cache. Returns None if the cache is invalid."""
    return deserialize_cache(cache_str, SETTINGS_EVENT_CACHE_SCHEMA)
