# stdlib
from json import JSONDecodeError, loads
from typing import Any, TypedDict

# 3p
from jsonschema import ValidationError, validate


class MetricBlobEntry(TypedDict, total=True):
    timestamp: float
    runtime: float
    resourceLogAmounts: dict[str, int]


METRIC_BLOB_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "Timeststamp": {"type": "number"},
        "Runtime": {"type": "number"},
        "ResourceLogAmounts": {"type": "object", "additionalProperties": {"type": "number"}},
    },
    "additionalProperties": False,
}


def validate_blob_metric_dict(blob_dict_str: str, oldest_legal_time: float) -> MetricBlobEntry | None:
    try:
        blob_dict: MetricBlobEntry = loads(blob_dict_str)
        validate(instance=blob_dict, schema=METRIC_BLOB_SCHEMA)
        # This is validated previously via the schema so this will always be legal
        if blob_dict["timestamp"] < oldest_legal_time:
            return None
        return blob_dict
    except (JSONDecodeError, ValidationError):
        return None
