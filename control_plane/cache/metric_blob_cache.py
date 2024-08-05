# stdlib
from json import JSONDecodeError, loads
from typing import Any, TypedDict

# 3p
from jsonschema import ValidationError, validate


class MetricBlobEntry(TypedDict, total=True):
    Name: str
    Value: float
    Time: float


class LogForwarderBlobMetrics(TypedDict, total=True):
    Values: list[MetricBlobEntry]


METRIC_BLOB_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "Values": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "Name": {"type": "string"},
                    "Value": {"type": "number"},
                    "Time": {"type": "number"},
                },
                "additionalProperties": False,
            },
        }
    },
    "additionalProperties": False,
}


def validate_blob_metric_dict(blob_dict_str: str, oldest_legal_time: float) -> LogForwarderBlobMetrics | None:
    try:
        blob_dict: LogForwarderBlobMetrics = loads(blob_dict_str)
        validate(instance=blob_dict, schema=METRIC_BLOB_SCHEMA)
        if len(blob_dict["Values"]) == 0:
            return None
        # This is validated previously via the schema so this will always be legal
        if blob_dict["Values"][0]["Time"] < oldest_legal_time:
            return None
        return blob_dict
    except (JSONDecodeError, ValidationError):
        return None
