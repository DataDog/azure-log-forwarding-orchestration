# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from dataclasses import dataclass
from typing import Any, Literal, TypeAlias

# project
from cache.common import deserialize_cache

ControlPlaneComponent: TypeAlias = Literal["resources", "scaling", "diagnostic_settings"]
ManifestCache: TypeAlias = dict[ControlPlaneComponent, str]
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

KEY_TO_ZIP: dict[ControlPlaneComponent, str] = {
    "resources": RESOURCES_TASK_ZIP,
    "scaling": SCALING_TASK_ZIP,
    "diagnostic_settings": DIAGNOSTIC_SETTINGS_TASK_ZIP,
}

ALL_ZIPS = frozenset(KEY_TO_ZIP.values())
ALL_COMPONENTS = frozenset(KEY_TO_ZIP)


PENDING_DEPLOYMENTS_CACHE_NAME = "pending_deployments.json"


@dataclass
class PendingDeployment:
    component: ControlPlaneComponent
    function_app: str
    poll_url: str
    target_manifest_hash: str


PendingDeploymentsCache: TypeAlias = dict[ControlPlaneComponent, PendingDeployment]

PENDING_DEPLOYMENT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "propertyNames": {"enum": ["resources", "scaling", "diagnostic_settings"]},
    "additionalProperties": {
        "type": "object",
        "properties": {
            "component": {"type": "string"},
            "function_app": {"type": "string"},
            "poll_url": {"type": "string"},
            "target_manifest_hash": {"type": "string"},
        },
        "required": ["component", "function_app", "poll_url", "target_manifest_hash"],
        "additionalProperties": False,
    },
}


def deserialize_pending_deployments(raw: str) -> PendingDeploymentsCache:
    result = deserialize_cache(raw, PENDING_DEPLOYMENT_SCHEMA)
    if not result:
        return {}
    return {k: PendingDeployment(**v) for k, v in result.items()}


def prune_manifest_cache(manifest_cache: ManifestCache) -> ManifestCache:
    """Remove any components from the manifest cache that are not valid components"""
    return {component: manifest_cache[component] for component in ALL_COMPONENTS if component in manifest_cache}


def deserialize_manifest_cache(raw_manifest_cache: str) -> ManifestCache | None:
    return deserialize_cache(raw_manifest_cache, MANIFEST_SCHEMA, prune_manifest_cache)
