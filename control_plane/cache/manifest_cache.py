# stdlib
from json import JSONDecodeError, loads
from typing import Any, TypeAlias

# 3p
from jsonschema import ValidationError, validate

ManifestCache: TypeAlias = dict[str, str]
"""
Mapping of deployable name to SHA-256 manifest
"""

MANIFEST_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "resources": {"type": "string"},
        "forwarder": {"type": "string"},
        "diagnostic_settings": {"type": "string"},
        "scaling": {"type": "string"},
    },
    "required": ["resources", "forwarder", "diagnostic_settings", "scaling"],
    "additionalProperties": False,
}

MANIFEST_CACHE_NAME = "manifest.json"


def deserialize_manifest_cache(raw_manifest_cache: str) -> ManifestCache | None:
    try:
        manifest_dict: ManifestCache = loads(raw_manifest_cache)
        validate(instance=manifest_dict, schema=MANIFEST_SCHEMA)
        return manifest_dict
    except (JSONDecodeError, ValidationError):
        return None
