from json import JSONDecodeError, loads
from typing import Any, TypeAlias, TypedDict

from jsonschema import ValidationError, validate

from cache.common import DIAGNOSTIC_SETTING_CONFIGURATION_SCHEMA, DiagnosticSettingConfiguration


ASSIGNMENT_CACHE_BLOB = "assignments.json"


class RegionAssignmentConfiguration(TypedDict, total=True):
    configurations: dict[str, DiagnosticSettingConfiguration]
    "Mapping of config_id to DiagnosticSettingConfiguration"
    resources: dict[str, str]
    "Mapping of resource_id to config_id"


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
                    "additionalProperties": DIAGNOSTIC_SETTING_CONFIGURATION_SCHEMA,
                },
                "resources": {
                    "type": "object",  # resource_id
                    "additionalProperties": {"type": "string"},  # config_id
                },
            },
            "required": ["configurations", "resources"],
            "additionalProperties": False,
        },
    },
}


def deserialize_assignment_cache(cache_str: str) -> tuple[bool, AssignmentCache]:
    try:
        cache = loads(cache_str)
        validate(instance=cache, schema=ASSIGNMENT_CACHE_SCHEMA)
        return True, cache
    except (JSONDecodeError, ValidationError):
        return False, {}
