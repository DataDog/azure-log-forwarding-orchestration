# stdlib
from typing import Any, TypedDict

# 3p
from cache.common import deserialize_cache


class MetricBlobEntry(TypedDict, total=True):
    """
    A representation of the metric blob entries
    """

    timestamp: float
    "a UNIX timestamp when metric was created"
    runtime_seconds: float
    "The number of seconds taken for the forwarder to run"
    resource_log_volume: dict[str, int]
    "A mapping of resource id ->log volume in number of logs"
    resource_log_bytes: dict[str, int]
    "A mapping of resource id ->log volume in number of bytes"
    forwarder_version: str | None
    "The version tag of the forwarder"


METRIC_NAMES = ("resource_log_volume", "resource_log_bytes", "runtime_seconds")

METRIC_BLOB_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "timestamp": {"type": "number"},
        "runtime_seconds": {"type": "number"},
        "resource_log_volume": {"type": "object", "additionalProperties": {"type": "number"}},
        "resource_log_bytes": {"type": "object", "additionalProperties": {"type": "number"}},
        "forwarder_version": {"type": "string"},
    },
    "required": ["timestamp", "runtime_seconds", "resource_log_volume", "resource_log_bytes", "forwarder_version"],
    "additionalProperties": False,
}


def deserialize_blob_metric_entry(raw_metric_entry: str, oldest_legal_time: float) -> MetricBlobEntry | None:
    def ensure_valid_timestamp(blob_dict: MetricBlobEntry) -> MetricBlobEntry | None:
        # This is validated previously via the schema so this will always be legal
        return None if blob_dict["timestamp"] < oldest_legal_time else blob_dict

    return deserialize_cache(raw_metric_entry, METRIC_BLOB_SCHEMA, ensure_valid_timestamp)
