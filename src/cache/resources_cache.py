from json import JSONDecodeError, loads
from typing import TypeAlias

from jsonschema import ValidationError, validate

from src.cache.common import UUID_REGEX

RESOURCE_CACHE_BLOB = "resources.json"


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
