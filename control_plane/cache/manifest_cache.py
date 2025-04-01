# stdlib
from typing import Any, Literal, TypeAlias

# project
from cache.common import deserialize_cache

ManifestKey: TypeAlias = Literal["resources", "scaling", "diagnostic_settings"]
ManifestCache: TypeAlias = dict[ManifestKey, str]
"""Mapping of deployable name to SHA-256 manifest"""


MANIFEST_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "resources": {"type": "string"},
        "scaling": {"type": "string"},
        "diagnostic_settings": {"type": "string"},
    },
    "required": ["resources", "diagnostic_settings", "scaling"],
    "additionalProperties": True,
}

MANIFEST_CACHE_NAME = "manifest.json"


PUBLIC_STORAGE_ACCOUNT_URL = "https://ddazurelfo.blob.core.windows.net"
TASKS_CONTAINER = "lfo"

RESOURCES_TASK_ZIP = "resources_task.zip"
SCALING_TASK_ZIP = "scaling_task.zip"
DIAGNOSTIC_SETTINGS_TASK_ZIP = "diagnostic_settings_task.zip"
MANIFEST_FILE_NAME = "manifest.json"

KEY_TO_ZIP: dict[ManifestKey, str] = {
    "resources": RESOURCES_TASK_ZIP,
    "scaling": SCALING_TASK_ZIP,
    "diagnostic_settings": DIAGNOSTIC_SETTINGS_TASK_ZIP,
}

ALL_ZIPS = list(KEY_TO_ZIP.values())


def deserialize_manifest_cache(raw_manifest_cache: str) -> ManifestCache | None:
    return deserialize_cache(raw_manifest_cache, MANIFEST_SCHEMA)
