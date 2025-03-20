# stdlib
from typing import Any, TypeAlias, TypedDict

# 3p
from cache.common import deserialize_cache

RESOURCE_CACHE_BLOB = "resources.json"


MONITORED_SUBSCRIPTIONS_SCHEMA: dict[str, Any] = {
    "type": "array",
    "items": {"type": "string"},
}


class ResourceMetadata(TypedDict, total=True):
    tags: list[str]
    filtered_in: bool


ResourceCacheV1: TypeAlias = dict[str, dict[str, set[str]]]
"""mapping of subscription_id to region to resource IDs"""

RegionToResourcesDict: TypeAlias = dict[str, dict[str, ResourceMetadata]]
ResourceCache: TypeAlias = dict[str, RegionToResourcesDict]
"""mapping of subscription_id to region to resource ID to resource metadata"""

TAGS_KEY = "tags"
FILTERED_IN_KEY = "filtered_in"
RESOURCE_CACHE_SCHEMA_V2: dict[str, Any] = {
    "type": "object",
    "propertyNames": {"format": "uuid"},
    "additionalProperties": {
        "type": "object",
        "additionalProperties": {
            "type": "object",
            "properties": {
                TAGS_KEY: {"type": "array", "items": {"type": "string"}},
                FILTERED_IN_KEY: {"type": "boolean"},
            },
        },
    },
}

RESOURCE_CACHE_SCHEMA_V1: dict[str, Any] = {
    "type": "object",
    "propertyNames": {"format": "uuid"},
    "additionalProperties": {"type": "object", "additionalProperties": {"type": "array", "items": {"type": "string"}}},
}


def read_resource_cache(cache_str: str) -> tuple[ResourceCache | None, bool]:
    """Read the resource cache and returns it in the v2 schema.
    If the existing cache is in the v1 schema, it will be upgraded to the v2 schema.
    Returns the cache and a bool indicating whether the caller should
    flush the cache because a schema upgrade occurred."""

    cache = deserialize_v2_resource_cache(cache_str)
    if cache is not None:
        return cache, False

    v1_cache = deserialize_v1_resource_cache(cache_str)
    if v1_cache is not None:
        return upgrade_cache_to_v2(v1_cache), True

    return None, False


def deserialize_monitored_subscriptions(env_str: str) -> list[str] | None:
    return deserialize_cache(env_str, MONITORED_SUBSCRIPTIONS_SCHEMA, lambda subs: [sub.lower() for sub in subs])


def deserialize_resource_tag_filters(tag_filter_str: str) -> list[str]:
    if len(tag_filter_str) == 0:
        return []

    tag_filters: list[str] = []
    parsed_tags = tag_filter_str.split(",")

    for parsed_tag in parsed_tags:
        tag = parsed_tag.strip().casefold()
        if len(tag) == 0:
            continue
        tag_filters.append(tag)

    return tag_filters


def deserialize_v2_resource_cache(cache_str: str) -> ResourceCache | None:
    """Deserialize the resource cache according to V2 schema. Returns None if the cache is invalid."""

    return deserialize_cache(cache_str, RESOURCE_CACHE_SCHEMA_V2)


def deserialize_v1_resource_cache(cache_str: str) -> ResourceCacheV1 | None:
    """Deserialize the resource cache according to V1 schema. Returns None if the cache is invalid."""

    return deserialize_cache(cache_str, RESOURCE_CACHE_SCHEMA_V1)


def is_v2_schema(cache: ResourceCache | None) -> bool:
    return cache is not None and all(
        isinstance(resources, dict)
        for resources_per_region in cache.values()
        for resources in resources_per_region.values()
    )


def upgrade_cache_to_v2(cache: ResourceCacheV1 | None) -> ResourceCache:
    if cache is None:
        return {}

    upgraded_cache: ResourceCache = {}

    for sub_id, resources_per_region in cache.items():
        upgraded_resources_per_region: dict[str, dict[str, ResourceMetadata]] = {}
        for region in resources_per_region:
            resources = resources_per_region[region]
            resource_metadatas: dict[str, ResourceMetadata] = {}
            for r in resources:
                metadata: ResourceMetadata = {TAGS_KEY: [], FILTERED_IN_KEY: True}
                resource_metadatas[r] = metadata
            upgraded_resources_per_region[region] = resource_metadatas
        upgraded_cache[sub_id] = upgraded_resources_per_region
    return upgraded_cache


def is_resource_filtered_in(cache: ResourceCache, sub_id: str, region: str, resource_id: str) -> bool:
    if cache and is_v2_schema(cache):
        return cache[sub_id][region][resource_id][FILTERED_IN_KEY]

    return False


def prune_resource_cache(cache: ResourceCache) -> None:
    """Prune the cache by removing empty regions and subscriptions."""
    for subscription_id, resources_per_region in list(cache.items()):
        for region, resources in list(resources_per_region.items()):
            if not resources:
                del resources_per_region[region]
        if not resources_per_region:
            del cache[subscription_id]
