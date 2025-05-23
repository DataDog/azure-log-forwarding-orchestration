# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

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
    include: bool


RegionToResourcesDict: TypeAlias = dict[str, dict[str, ResourceMetadata]]
"mapping of region name to dict where key=resource_id and value=resource metadata"

ResourceCache: TypeAlias = dict[str, RegionToResourcesDict]
"mapping of subscription_id to RegionToResourcesDict"

ResourceCacheV1: TypeAlias = dict[str, dict[str, set[str]]]
"mapping of subscription_id to region to set of resource_id strings"

INCLUDE_KEY = "include"
RESOURCE_CACHE_SCHEMA_V2: dict[str, Any] = {
    "type": "object",
    "propertyNames": {"format": "uuid"},
    "additionalProperties": {
        "type": "object",
        "additionalProperties": {
            "type": "object",
            "properties": {
                INCLUDE_KEY: {"type": "boolean"},
            },
        },
    },
}

RESOURCE_CACHE_SCHEMA_V1: dict[str, Any] = {
    "type": "object",
    "propertyNames": {"format": "uuid"},
    "additionalProperties": {"type": "object", "additionalProperties": {"type": "array", "items": {"type": "string"}}},
}


def deserialize_resource_cache(cache_str: str) -> tuple[ResourceCache | None, bool]:
    """Read the resource cache and returns it in the v2 schema.
    If the existing cache is in the v1 schema, it will be upgraded to the v2 schema.
    Returns the cache and a bool indicating whether the caller should
    flush the cache because a schema upgrade occurred."""

    cache = _deserialize_v2_resource_cache(cache_str)
    if cache is not None:
        return cache, False

    v1_cache = _deserialize_v1_resource_cache(cache_str)
    if v1_cache is not None:
        return upgrade_cache_to_v2(v1_cache), True

    return None, False


def deserialize_monitored_subscriptions(env_str: str) -> list[str] | None:
    """Deserialize monitored subscriptions from a string representation of an array of subscription IDs"""

    return deserialize_cache(env_str, MONITORED_SUBSCRIPTIONS_SCHEMA, lambda subs: [sub.lower() for sub in subs])


def deserialize_resource_tag_filters(tag_filter_str: str) -> list[str]:
    """Deserialize resource tag filters from a CSV string"""

    return [tag.strip().casefold() for tag in tag_filter_str.split(",") if len(tag) > 0]


def _deserialize_v2_resource_cache(cache_str: str) -> ResourceCache | None:
    """Deserialize the resource cache according to V2 schema. Returns None if the cache is invalid."""

    return deserialize_cache(cache_str, RESOURCE_CACHE_SCHEMA_V2)


def _deserialize_v1_resource_cache(cache_str: str) -> ResourceCacheV1 | None:
    """Deserialize the resource cache according to V1 schema. Returns None if the cache is invalid."""

    return deserialize_cache(cache_str, RESOURCE_CACHE_SCHEMA_V1)


def upgrade_cache_to_v2(cache: ResourceCacheV1 | None) -> ResourceCache:
    """Upgrades resource cache from v1 to v2 schema.
    v1 schema -> each region maps to a set of resource IDs
    v2 schema -> each region maps to a dict where key=resource ID and value=resource metadata
    The default value for a new resource metadata is { include=True } to be backwards-compatible
    Returns the upgraded cache according to the v2 schema.
    """
    if not cache:
        return {}

    return {
        sub_id: {
            region: {resource_id: ResourceMetadata(include=True) for resource_id in resources}
            for region, resources in resources_per_region.items()
        }
        for sub_id, resources_per_region in cache.items()
    }


def prune_resource_cache(cache: ResourceCache) -> None:
    """Prune the cache by removing empty regions and subscriptions."""
    for subscription_id, resources_per_region in list(cache.items()):
        for region, resources in list(resources_per_region.items()):
            if not resources:
                del resources_per_region[region]
        if not resources_per_region:
            del cache[subscription_id]
