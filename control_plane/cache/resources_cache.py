# stdlib
from typing import Any, TypeAlias, TypedDict, cast

# 3p
from cache.common import deserialize_cache

RESOURCE_CACHE_BLOB = "resources.json"


MONITORED_SUBSCRIPTIONS_SCHEMA: dict[str, Any] = {
    "type": "array",
    "items": {"type": "string"},
}


class ResourceMetadata(TypedDict, total=True):
    tags: list[str]
    filtered_out: bool


RegionToResourcesDict: TypeAlias = dict[str, set[str] | dict[str, ResourceMetadata]]
ResourceCache: TypeAlias = dict[str, RegionToResourcesDict]
"mapping of subscription_id to region to resources"

RESOURCE_CACHE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "propertyNames": {"format": "uuid"},
    "additionalProperties": {
        "type": "object",
        "additionalProperties": {
            "oneOf": [
                {"type": "array", "items": {"type": "string"}},
                {
                    "type": "object",
                    "properties": {
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "filtered_out": {"type": "boolean"},
                    },
                },
            ]
        },
    },
}


def deserialize_monitored_subscriptions(env_str: str) -> list[str] | None:
    return deserialize_cache(env_str, MONITORED_SUBSCRIPTIONS_SCHEMA, lambda subs: [sub.lower() for sub in subs])


def deserialize_resource_cache(cache_str: str) -> ResourceCache | None:
    """Deserialize the resource cache. Returns None if the cache is invalid."""

    def convert_resource_ids_to_metadata(cache: ResourceCache) -> ResourceCache:
        for sub_id, resources_per_region in cache.items():
            for region in resources_per_region:
                resources = resources_per_region[region]
                resource_metadatas: dict[str, ResourceMetadata] = {}
                if isinstance(resources, dict):
                    for resource_id, resource_metadata in resources.items():
                        if isinstance(resource_metadata, dict):
                            resource_metadata = cast(ResourceMetadata, resource_metadata)
                            resource_metadatas[resource_id] = resource_metadata
                        else:
                            metadata: ResourceMetadata = {"tags": [], "filtered_out": False}
                            resource_metadatas[resource_id] = metadata
                else:
                    for r in resources:
                        metadata: ResourceMetadata = {"tags": [], "filtered_out": False}
                        resource_metadatas[r] = metadata
                resources_per_region[region] = resource_metadatas
            cache[sub_id] = resources_per_region
        return cache

    return deserialize_cache(cache_str, RESOURCE_CACHE_SCHEMA, convert_resource_ids_to_metadata)


def prune_resource_cache(cache: ResourceCache) -> None:
    """Prune the cache by removing empty regions and subscriptions."""
    for subscription_id, resources_per_region in list(cache.items()):
        for region, resources in list(resources_per_region.items()):
            if not resources:
                del resources_per_region[region]
        if not resources_per_region:
            del cache[subscription_id]
