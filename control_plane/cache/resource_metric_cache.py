from json import JSONDecodeError, loads
from typing import Any, TypeAlias

from jsonschema import ValidationError, validate




ResourceMetricCache: TypeAlias = dict[str, dict[str, int | float]]
"Mapping of resource_id to metric_name to value"


RESOURCE_METRIC_CACHE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "patternProperties": {
        "^S_": {
            "type": "object",
            "patternProperties": {
                "^S_": {"type": "number"}
            },
            "additionalProperties": False
        }
    },
    "additionalProperties": False
}


def deserialize_diagnostic_settings_cache(cache_str: str) -> tuple[bool, ResourceMetricCache]:
    try:
        cache = loads(cache_str)
        validate(instance=cache, schema=RESOURCE_METRIC_CACHE_SCHEMA)
        return True, cache
    except (JSONDecodeError, ValidationError):
        return False, {}
