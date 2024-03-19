from json import JSONDecodeError, loads
from logging import getLogger
from typing import Collection, Mapping, TypeAlias, TypedDict


from jsonschema import ValidationError, validate


log = getLogger(__name__)


SubscriptionId: TypeAlias = str
ResourceId: TypeAlias = str
ResourceCache: TypeAlias = Mapping[SubscriptionId, Collection[ResourceId]]


UUID_REGEX = r"^[a-f0-9]{8}-([a-f0-9]{4}-){3}[a-f0-9]{12}$"

RESOURCE_CACHE_SCHEMA = {
    "type": "object",
    "patternProperties": {
        UUID_REGEX: {"type": "array", "items": {"type": "string"}},
    },
}


class ResourceCacheError(Exception):
    def __init__(self) -> None:
        super().__init__("Resource cache is in an invalid format.")


def deserialize_resource_cache(cache_str: str) -> ResourceCache:
    try:
        cache = loads(cache_str)
        validate(instance=cache, schema=RESOURCE_CACHE_SCHEMA)
    except (JSONDecodeError, ValidationError):
        raise ResourceCacheError()
    return {sub_id: frozenset(resources) for sub_id, resources in cache.items()}


DIAGNOSTIC_SETTINGS_CACHE_ERROR_MSG = "Diagnostic settings cache is in an invalid format, task will reset the cache"


class ResourceConfiguration(TypedDict):
    diagnostic_setting_id: str
    event_hub_name: str
    event_hub_namespace: str


DiagnosticSettingsCache: TypeAlias = dict[SubscriptionId, dict[ResourceId, ResourceConfiguration]]

DIAGNOSTIC_SETTINGS_CACHE_SCHEMA = {
    "type": "object",
    "patternProperties": {
        UUID_REGEX: {
            "type": "object",
            "patternProperties": {
                ".*": {
                    "type": "object",
                    "properties": {
                        "diagnostic_setting_id": {"type": "string"},
                        "event_hub_name": {"type": "string"},
                        "event_hub_namespace": {"type": "string"},
                    },
                    "required": ["diagnostic_setting_id", "event_hub_name", "event_hub_namespace"],
                    "additionalProperties": False,
                },
            },
        }
    },
}


def deserialize_diagnostic_settings_cache(cache_str: str) -> DiagnosticSettingsCache:
    try:
        cache = loads(cache_str)
        validate(instance=cache, schema=DIAGNOSTIC_SETTINGS_CACHE_SCHEMA)
        return cache
    except Exception:
        pass
    log.warning(DIAGNOSTIC_SETTINGS_CACHE_ERROR_MSG)
    return {}
