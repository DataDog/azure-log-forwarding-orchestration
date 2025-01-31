# stdlib
from typing import Any, Literal, TypeAlias, TypedDict

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


PUBLIC_STORAGE_ACCOUNT_URL = "https://ddazurelfo.blob.core.windows.net"
TASKS_CONTAINER = "lfo"

RESOURCES_TASK_ZIP = "resources_task.zip"
SCALING_TASK_ZIP = "scaling_task.zip"
DIAGNOSTIC_SETTINGS_TASK_ZIP = "diagnostic_settings_task.zip"
FORWARDER_ZIP = "forwarder.zip"
MANIFEST_FILE_NAME = "manifest.json"

ALL_ZIPS = [RESOURCES_TASK_ZIP, SCALING_TASK_ZIP, DIAGNOSTIC_SETTINGS_TASK_ZIP, FORWARDER_ZIP]

KEY_TO_ZIP = {
    "resources": RESOURCES_TASK_ZIP,
    "scaling": SCALING_TASK_ZIP,
    "diagnostic_settings": DIAGNOSTIC_SETTINGS_TASK_ZIP,
    "forwarder": FORWARDER_ZIP,
}


def deserialize_manifest_cache(raw_manifest_cache: str) -> ManifestCache | None:
    return deserialize_cache(raw_manifest_cache, MANIFEST_SCHEMA)
