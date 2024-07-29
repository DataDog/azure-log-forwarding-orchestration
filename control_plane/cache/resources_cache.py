# stdlib
from json import JSONDecodeError, loads
from typing import Any, TypeAlias

# 3p
from jsonschema import ValidationError, validate

RESOURCE_CACHE_BLOB = "resources.json"


ResourceCache: TypeAlias = dict[str, dict[str, set[str]]]
"mapping of subscription_id to region to resource_ids"


RESOURCE_CACHE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "propertyNames": {"format": "uuid"},
    "additionalProperties": {"type": "object", "additionalProperties": {"type": "array", "items": {"type": "string"}}},
}


def deserialize_resource_cache(cache_str: str) -> tuple[bool, ResourceCache]:
    """Deserialize the resource cache, returning a tuple of success and the cache dict."""
    try:
        cache = loads(cache_str)
        validate(instance=cache, schema=RESOURCE_CACHE_SCHEMA)
        # Convert the list of resources to a set
        for resources_per_region in cache.values():
            for region in resources_per_region.keys():
                resources_per_region[region] = set(resources_per_region[region])
        return True, cache
    except (JSONDecodeError, ValidationError):
        return False, {}
