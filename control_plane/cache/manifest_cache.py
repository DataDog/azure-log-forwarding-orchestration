# stdlib
from typing import Any, Literal, TypeAlias, TypedDict

# project
from cache.common import InvalidCacheError, deserialize_cache

# manifest example:
# {
#     "latest": {
#         "scaling": "v{pipeline-id}-{git-sha}",
#         "resources": "v{pipeline-id}-{git-sha}",
#         "diagnostic_settings": "v{pipeline-id}-{git-sha}",
#     },
#     "version_history": {
#         "v{pipeline-id}-{git-sha}": {
#             "scaling": "b9408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
#             "resources": "b9408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
#             "diagnostic_settings": "b9408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d"
#         },
#         "v{pipeline-id}-{git-sha}": {
#             "scaling": "b9408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
#             "resources": "b9408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
#             "diagnostic_settings": "b9408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d"
#         },
#         "v{pipeline-id}-{git-sha}": {
#             "scaling": "b9408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
#             "resources": "b9408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
#             "diagnostic_settings": "b9408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d"
#         }
#     }
# }


DeployableName: TypeAlias = Literal["resources", "scaling", "diagnostic_settings"]


Versions: TypeAlias = dict[DeployableName, str]
"Mapping of deployable name to version (either a hash or a version tag)"


class PublicManifest(TypedDict, total=True):
    latest: Versions
    version_history: dict[str, Versions]


def get_versions_schema(version_regex: str) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "resources": {"type": "string", "pattern": version_regex},
            "scaling": {"type": "string", "pattern": version_regex},
            "diagnostic_settings": {"type": "string", "pattern": version_regex},
        },
        "additionalProperties": False,
    }


VERSION_TAG_REGEX = r"^v[0-9]+-[0-9a-f]+$"
HASH_REGEX = r"^[0-9a-f]{64}$"

MANIFEST_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "latest": get_versions_schema(VERSION_TAG_REGEX),
        "version_history": {
            "type": "object",
            "patternProperties": {
                VERSION_TAG_REGEX: get_versions_schema(HASH_REGEX),
            },
        },
    },
    "required": ["latest", "version_history"],
    "additionalProperties": False,
}

MANIFEST_NAME = "manifest.json"


PUBLIC_STORAGE_ACCOUNT_URL = "https://ddazurelfo.blob.core.windows.net"
TASKS_CONTAINER = "lfo"

RESOURCES = "resources"
SCALING = "scaling"
DIAGNOSTIC_SETTINGS = "diagnostic_settings"
ALL_DEPLOYABLES = [RESOURCES, SCALING, DIAGNOSTIC_SETTINGS]
MANIFEST_FILE_NAME = "manifest.json"


def get_task_zip_name(deployable: str, version_tag: str) -> str:
    return f"{deployable}_task_{version_tag}.zip"


def deserialize_public_manifest(raw_manifest: str) -> PublicManifest | None:
    def _validate_public_manifest(manifest: PublicManifest) -> PublicManifest:
        if not all(version in manifest["version_history"] for version in manifest["latest"].values()):
            raise InvalidCacheError("Latest version not in version history")
        if not all(deployable in manifest["latest"] for deployable in ALL_DEPLOYABLES):
            raise InvalidCacheError("`latest` does not contain all deployables")
        return manifest

    return deserialize_cache(raw_manifest, MANIFEST_SCHEMA, _validate_public_manifest)


ManifestCache: TypeAlias = Versions
"Private cache of versions currently deployed"
MANIFEST_CACHE_SCHEMA = get_versions_schema(VERSION_TAG_REGEX)


def deserialize_manifest_cache(raw_manifest_cache: str) -> ManifestCache | None:
    return deserialize_cache(raw_manifest_cache, MANIFEST_CACHE_SCHEMA)
