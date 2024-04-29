from json import JSONDecodeError, loads
from typing import TypeAlias, TypedDict

from jsonschema import ValidationError, validate


### COPY BELOW ###

### cache/diagnostic_settings_cache.py


UUID_REGEX = r"^[a-f0-9]{8}-([a-f0-9]{4}-){3}[a-f0-9]{12}$"


class DiagnosticSettingConfiguration(TypedDict):
    id: str
    event_hub_name: str
    event_hub_namespace: str


DiagnosticSettingsCache: TypeAlias = dict[str, dict[str, DiagnosticSettingConfiguration]]
"Mapping of subscription_id to resource_id to DiagnosticSettingConfiguration"

DIAGNOSTIC_SETTINGS_CACHE_SCHEMA = {
    "type": "object",
    "patternProperties": {
        UUID_REGEX: {
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
        }
    },
}


def deserialize_diagnostic_settings_cache(cache_str: str) -> tuple[bool, DiagnosticSettingsCache]:
    try:
        cache = loads(cache_str)
        validate(instance=cache, schema=DIAGNOSTIC_SETTINGS_CACHE_SCHEMA)
        return True, cache
    except (JSONDecodeError, ValidationError):
        return False, {}


### cache/resources_cache.py

ResourceCache: TypeAlias = dict[str, set[str]]
"mapping of subscription_id to resource_ids"


RESOURCE_CACHE_SCHEMA = {
    "type": "object",
    "patternProperties": {
        UUID_REGEX: {"type": "array", "items": {"type": "string"}},
    },
}


def deserialize_resource_cache(cache_str: str) -> tuple[bool, ResourceCache]:
    """Deserialize the resource cache, returning a tuple of success and the cache dict."""
    try:
        cache = loads(cache_str)
        validate(instance=cache, schema=RESOURCE_CACHE_SCHEMA)
        return True, {sub_id: set(resources) for sub_id, resources in cache.items()}
    except (JSONDecodeError, ValidationError):
        return False, {}
