# stdlib
from json import JSONDecodeError, loads
from typing import Any, TypedDict

# 3p
from jsonschema import ValidationError, validate


class MetricBlobEntry(TypedDict, total=True):
    """
    A representation of the metric blob entries
    """

    timestamp: float
    "a UNIX timestamp when metric was created"
    runtimeSeconds: float
    "runtimeSeconds: The number of seconds taken for the forwarder to run"
    resourceLogVolumes: dict[str, int]
    "resourceLogVolumes: A mapping of resource id ->log volume in bytes"


METRIC_BLOB_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "timestamp": {"type": "number"},
        "runtimeSeconds": {"type": "number"},
        "resourceLogVolumes": {"type": "object", "additionalProperties": {"type": "number"}},
    },
    "required": ["timestamp", "runtimeSeconds", "resourceLogVolumes"],
    "additionalProperties": False,
}


def deserialize_blob_metric_entry(raw_metric_entry: str, oldest_legal_time: float) -> MetricBlobEntry | None:
    try:
        blob_dict: MetricBlobEntry = loads(raw_metric_entry)
        validate(instance=blob_dict, schema=METRIC_BLOB_SCHEMA)
        # This is validated previously via the schema so this will always be legal
        if blob_dict["timestamp"] < oldest_legal_time:
            return None
        return blob_dict
    except (JSONDecodeError, ValidationError):
        return None
