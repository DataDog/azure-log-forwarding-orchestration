from json import JSONDecodeError, loads
from logging import getLogger
from typing import Any, Collection, Mapping, TypeAlias, TypeGuard, TypedDict

log = getLogger(__name__)


SubscriptionId: TypeAlias = str
ResourceId: TypeAlias = str
ResourceCache: TypeAlias = Mapping[SubscriptionId, Collection[ResourceId]]


class ResourceCacheError(Exception):
    def __init__(self) -> None:
        super().__init__("Resource cache is in an invalid format.")


def deserialize_resource_cache(cache_str: str) -> ResourceCache:
    try:
        cache = loads(cache_str)
    except JSONDecodeError:
        raise ResourceCacheError()
    if not isinstance(cache, dict):
        raise ResourceCacheError()
    for sub_id, resources in cache.items():
        if not isinstance(resources, list):
            raise ResourceCacheError()
        cache[sub_id] = frozenset(resources)
    return cache


DIAGNOSTIC_SETTINGS_CACHE_ERROR_MSG = "Diagnostic settings cache is in an invalid format, task will reset the cache"


class ResourceConfiguration(TypedDict):
    diagnostic_setting_id: str
    event_hub_name: str
    event_hub_namespace: str


DiagnosticSettingsCache: TypeAlias = dict[SubscriptionId, dict[ResourceId, ResourceConfiguration]]


def _is_diagnostic_settings_cache(cache: Any) -> TypeGuard[DiagnosticSettingsCache]:
    return isinstance(cache, dict) and all(
        isinstance(sub_id, str)
        and isinstance(resources, dict)
        and all(
            isinstance(resource_id, str)
            and isinstance(config, dict)
            and all(key in config for key in ("diagnostic_setting_id", "event_hub_name", "event_hub_namespace"))
            for resource_id, config in resources.values()
        )
        for sub_id, resources in cache.values()
    )


def deserialize_diagnostic_settings_cache(cache_str: str) -> DiagnosticSettingsCache:
    try:
        cache = loads(cache_str)
        if _is_diagnostic_settings_cache(cache):
            return cache
    except Exception:
        pass
    log.warning(DIAGNOSTIC_SETTINGS_CACHE_ERROR_MSG)
    return {}
