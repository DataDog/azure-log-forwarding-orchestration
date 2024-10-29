#!/usr/bin/env python3

# stdlib
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from json import dumps
from logging import WARNING, getLogger, basicConfig, INFO
from os import getenv
import sys

# 3p
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContainerClient, ContentSettings

from cache.manifest_cache import (
    PublicManifest,
    RESOURCES,
    SCALING,
    DIAGNOSTIC_SETTINGS,
    MANIFEST_FILE_NAME,
    TASKS_CONTAINER,
    DeployableName,
    Versions,
    deserialize_public_manifest,
    get_task_zip_name,
)

if len(sys.argv) < 2:
    print("Usage: publish.py <public_storage_account_url>")
    sys.exit(1)

PUBLIC_STORAGE_ACCOUNT_URL = sys.argv[1]
CI_PIPELINE_ID = getenv("CI_PIPELINE_ID")
CI_COMMIT_SHORT_SHA = getenv("CI_COMMIT_SHORT_SHA")
VERSION_TAG = f"v{CI_PIPELINE_ID}-{CI_COMMIT_SHORT_SHA}"

basicConfig(level=INFO)
log = getLogger("publish")
getLogger("azure").setLevel(WARNING)

log.info("Reading zip files from dist/ and generating hashes")

BUILT_ZIPS: dict[DeployableName, str] = {
    RESOURCES: "resources_task.zip",
    SCALING: "scaling_task.zip",
    DIAGNOSTIC_SETTINGS: "diagnostic_settings_task.zip",
}

deployable_data: dict[DeployableName, tuple[str, bytes]] = {}
for deployable, file in BUILT_ZIPS.items():
    with open(f"dist/{file}", "rb") as z:
        content = z.read()
        deployable_data[deployable] = (sha256(content).hexdigest(), content)

log.info(
    "Uploading the following zip files to %s/%s:\n%s",
    PUBLIC_STORAGE_ACCOUNT_URL,
    TASKS_CONTAINER,
    "\n".join(
        get_task_zip_name(deployable, VERSION_TAG) for deployable in deployable_data
    ),
)

with (
    DefaultAzureCredential() as cred,
    ContainerClient(PUBLIC_STORAGE_ACCOUNT_URL, TASKS_CONTAINER, cred) as client,
    ThreadPoolExecutor() as executor,
):
    if not client.exists():
        log.warning("Container %s does not exist, creating it...", TASKS_CONTAINER)
        client.create_container()
    futures = [
        executor.submit(
            client.upload_blob,
            get_task_zip_name(deployable, VERSION_TAG),
            data,
            overwrite=True,
        )
        for deployable, (_, data) in deployable_data.items()
    ]
    exceptions = [e for f in futures if (e := f.exception())]
    for e in exceptions:
        log.error("An error occurred while uploading a zip file", exc_info=e)
    if exceptions:
        log.error("Exiting due to errors, will not update manifest")
        raise SystemExit(1)
    try:
        manifest_raw = client.download_blob(
            MANIFEST_FILE_NAME, encoding="UTF-8"
        ).readall()
        manifest: PublicManifest | None = deserialize_public_manifest(manifest_raw)
    except ResourceNotFoundError:
        manifest = None
    latest: Versions = {deployable: VERSION_TAG for deployable in deployable_data}  # type: ignore
    hashes: Versions = {
        deployable: content_hash
        for deployable, (content_hash, _) in deployable_data.items()  # type: ignore
    }
    if manifest is None:
        log.warning("Failed to deserialize manifest, resetting")
        manifest = {
            "latest": latest,
            "version_history": {VERSION_TAG: hashes},
        }
    else:
        manifest["latest"] = latest
        manifest["version_history"][VERSION_TAG] = hashes

    client.upload_blob(
        MANIFEST_FILE_NAME,
        dumps(manifest),
        overwrite=True,
        content_settings=ContentSettings(content_encoding="UTF-8"),
    )

log.info(
    "Done uploading zip files to %s/%s", PUBLIC_STORAGE_ACCOUNT_URL, TASKS_CONTAINER
)
