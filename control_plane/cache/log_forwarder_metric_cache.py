# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/  Copyright 2025 Datadog, Inc.

from json import JSONDecodeError, loads
from typing import Any, TypeAlias

from jsonschema import ValidationError, validate

LogForwarderMetricCache: TypeAlias = dict[str, dict[str, float]]
"""
Mapping of resource id to metric name to metric max value
Metric units depend on the metric.
"""


LOG_FORWARDER_METRIC_CACHE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": {"type": "object", "additionalProperties": {"type": "number"}},
}


def deserialize_log_forwarder_metrics_cache(cache_str: str) -> tuple[bool, LogForwarderMetricCache]:
    try:
        cache = loads(cache_str)
        validate(instance=cache, schema=LOG_FORWARDER_METRIC_CACHE_SCHEMA)
        return True, cache
    except (JSONDecodeError, ValidationError):
        return False, {}
