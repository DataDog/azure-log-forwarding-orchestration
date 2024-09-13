# stdlib
from typing import Any, Literal, TypeAlias, TypedDict

# 3p
# project
from cache.common import deserialize_cache

ManifestKey: TypeAlias = Literal["forwarder", "resources", "scaling", "diagnostic_settings"]


class ManifestCache(TypedDict, total=True):
    """
    Mapping of deployable name to SHA-256 manifest
    """

    forwarder: str
    resources: str
    scaling: str
    diagnostic_settings: str


MANIFEST_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "forwarder": {"type": "string"},
        "resources": {"type": "string"},
        "scaling": {"type": "string"},
        "diagnostic_settings": {"type": "string"},
    },
    "required": ["resources", "forwarder", "diagnostic_settings", "scaling"],
    "additionalProperties": False,
}

MANIFEST_CACHE_NAME = "manifest.json"


def deserialize_manifest_cache(raw_manifest_cache: str) -> ManifestCache | None:
    return deserialize_cache(raw_manifest_cache, MANIFEST_SCHEMA)
