# stdlib
from typing import Any, TypeAlias

# 3p
from cache.common import deserialize_cache

RESOURCE_CACHE_BLOB = "resources.json"


ResourceCache: TypeAlias = dict[str, dict[str, set[str]]]
"mapping of subscription_id to region to resource_ids"


RESOURCE_CACHE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "propertyNames": {"format": "uuid"},
    "additionalProperties": {"type": "object", "additionalProperties": {"type": "array", "items": {"type": "string"}}},
}


def deserialize_resource_cache(cache_str: str) -> ResourceCache | None:
    """Deserialize the resource cache. Returns None if the cache is invalid."""

    def convert_resources_to_set(cache: ResourceCache) -> ResourceCache:
        for resources_per_region in cache.values():
            for region in resources_per_region:
                resources_per_region[region] = set(resources_per_region[region])
        return cache

    return deserialize_cache(cache_str, RESOURCE_CACHE_SCHEMA, convert_resources_to_set)
