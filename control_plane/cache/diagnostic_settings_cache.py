# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from typing import Any, TypeAlias, TypedDict

# project
from cache.assignment_cache import AssignmentCache
from cache.common import deserialize_cache

EVENT_CACHE_BLOB = "settings_event.json"
DIAGNOSTIC_SETTINGS_COUNT = "diagnostic_settings_count"
SENT_EVENT = "sent_event"


class EventDict(TypedDict):
    diagnostic_settings_count: int
    sent_event: bool


DiagnosticSettingsCache: TypeAlias = dict[str, dict[str, EventDict]]
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
    "propertyNames": {"format": "uuid"},  # subscription_id
    "additionalProperties": {
        "type": "object",  # resource_id
        "additionalProperties": {
            "type": "object",
            "properties": {
                "diagnostic_settings_count": {"type": "integer"},
                "sent_event": {"type": "boolean"},
            },
            "required": ["diagnostic_settings_count", "sent_event"],
        },
    },
}


def deserialize_event_cache(cache_str: str) -> DiagnosticSettingsCache | None:
    """Deserialize the diagnostic settings event cache. Returns None if the cache is invalid."""
    return deserialize_cache(cache_str, SETTINGS_EVENT_CACHE_SCHEMA)


def remove_cached_resource(cache: DiagnosticSettingsCache, sub_id: str, resource_id: str):
    if sub_id in cache:
        cache[sub_id].pop(resource_id, None)


def update_cached_event(
    cache: DiagnosticSettingsCache, sub_id: str, resource_id: str, num_diag_settings: int, sent_event: bool
):
    cache.setdefault(sub_id, {})[resource_id] = EventDict(
        diagnostic_settings_count=num_diag_settings, sent_event=sent_event
    )


def prune_event_cache(cache: DiagnosticSettingsCache, assignment_cache: AssignmentCache) -> DiagnosticSettingsCache:
    """Returns a copy of the event cache containing only resources that are still in the assignment cache.

    The scaling task drops resources from the assignment cache once they are no longer discovered
    (deleted, or in a subscription that is no longer monitored), so their entries here are stale.
    """
    assigned_resources = {
        sub_id: {resource_id for region_config in regions.values() for resource_id in region_config["resources"]}
        for sub_id, regions in assignment_cache.items()
    }
    return {
        sub_id: {
            resource_id: EventDict(**event)
            for resource_id, event in resources.items()
            if resource_id in assigned_resources[sub_id]
        }
        for sub_id, resources in cache.items()
        if sub_id in assigned_resources
    }
