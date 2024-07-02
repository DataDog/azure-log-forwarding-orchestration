from json import JSONDecodeError, loads
from typing import Any, TypeAlias

from jsonschema import ValidationError, validate

from cache.common import DIAGNOSTIC_SETTING_CONFIGURATION_SCHEMA, DiagnosticSettingConfiguration


ASSIGNMENT_CACHE_BLOB = "assignments.json"


AssignmentCache: TypeAlias = dict[str, dict[str, dict[str, DiagnosticSettingConfiguration]]]
"Mapping of subscription_id to region to resource_id to DiagnosticSettingConfiguration"


ASSIGNMENT_CACHE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "propertyNames": {"format": "uuid"},  # subscription_id
    "additionalProperties": {
        "type": "object",  # region
        "additionalProperties": {
            "type": "object",  # resource_id
            "additionalProperties": DIAGNOSTIC_SETTING_CONFIGURATION_SCHEMA,
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
