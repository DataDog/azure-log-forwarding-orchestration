# stdlib
from typing import Any, NotRequired, TypeAlias, TypedDict

# 3p
from jsonschema import ValidationError

# project
from cache.common import LOG_FORWARDER_TYPE_SCHEMA, LogForwarderType, deserialize_cache

ASSIGNMENT_CACHE_BLOB = "assignments.json"


class RegionAssignmentConfiguration(TypedDict, total=True):
    configurations: dict[str, LogForwarderType]
    "Mapping of config_id to DiagnosticSettingType"
    resources: dict[str, str]
    "Mapping of resource_id to config_id"
    on_cooldown: NotRequired[bool]
    "whether the region has recently scaled up and is on cooldown"


AssignmentCache: TypeAlias = dict[str, dict[str, RegionAssignmentConfiguration]]
"Mapping of subscription_id to region to RegionAssignmentConfig"


ASSIGNMENT_CACHE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "propertyNames": {"format": "uuid"},  # subscription_id
    "additionalProperties": {
        "type": "object",  # region name
        "additionalProperties": {
            "properties": {  # region config
                "configurations": {
                    "type": "object",  # config_id
                    "additionalProperties": LOG_FORWARDER_TYPE_SCHEMA,
                },
                "resources": {
                    "type": "object",  # resource_id
                    "additionalProperties": {"type": "string"},  # config_id
                },
                "on_cooldown": {"type": "boolean"},
            },
            "required": ["configurations", "resources"],
            "additionalProperties": False,
        },
    },
}


def _validate_valid_config_ids(cache: AssignmentCache) -> AssignmentCache:
    for region_configs in cache.values():
        for region_config in region_configs.values():
            configs = region_config["configurations"]
            for config_id in region_config["resources"].values():
                if config_id not in configs:
                    raise ValidationError(f"Config ID {config_id} not found in region configurations")
    return cache


def deserialize_assignment_cache(cache_str: str) -> AssignmentCache | None:
    """Deserialize the assignment cache. Returns None if the cache is invalid."""
    return deserialize_cache(cache_str, ASSIGNMENT_CACHE_SCHEMA, _validate_valid_config_ids)
