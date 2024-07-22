from json import JSONDecodeError, loads
from typing import Any, TypeAlias

from jsonschema import ValidationError, validate


# Mapping of resource_id to metric_name to max value of a metric.
# Value is metric specific but could represent units such as seconds, or counld represnt counts.
ResourceMetricCache: TypeAlias = dict[str, dict[str, int | float]]


RESOURCE_METRIC_CACHE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": {"type": "object", "additionalProperties": {"type": "number"}},
}


def deserialize_diagnostic_settings_cache(cache_str: str) -> tuple[bool, ResourceMetricCache]:
    try:
        cache = loads(cache_str)
        validate(instance=cache, schema=RESOURCE_METRIC_CACHE_SCHEMA)
        return True, cache
    except (JSONDecodeError, ValidationError):
        return False, {}
