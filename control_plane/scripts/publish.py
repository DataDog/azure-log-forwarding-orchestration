#!/usr/bin/env python3
# usage: publish.py <public_storage_account_url> [connection_string]

# stdlib
import sys
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from json import dumps
from logging import INFO, WARNING, basicConfig, getLogger

# 3p
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

from cache.manifest_cache import (
    ALL_ZIPS,
    DIAGNOSTIC_SETTINGS_TASK_ZIP,
    MANIFEST_FILE_NAME,
    RESOURCES_TASK_ZIP,
    SCALING_TASK_ZIP,
    TASKS_CONTAINER,
    ManifestCache,
)

if len(sys.argv) < 2:
    print("Usage: publish.py <public_storage_account_url>")
    sys.exit(1)

storage_account_url = sys.argv[1]

basicConfig(level=INFO)
log = getLogger("publish")
getLogger("azure").setLevel(WARNING)

log.info("Reading artifacts from dist/")
files: dict[str, bytes] = {}
for filename in ALL_ZIPS + ["initial_run.sh"]:
    with open(f"dist/{filename}", "rb") as f:
        files[filename] = f.read()

log.info("Generating Hashes for the files")
hashes: ManifestCache = {
    "resources": sha256(files[RESOURCES_TASK_ZIP]).hexdigest(),
    "scaling": sha256(files[SCALING_TASK_ZIP]).hexdigest(),
    "diagnostic_settings": sha256(files[DIAGNOSTIC_SETTINGS_TASK_ZIP]).hexdigest(),
    "forwarder": "",  # TODO(AZINTS-2780)
}

log.info(
    "Uploading the following files to %s/%s:\n%s",
    storage_account_url,
    TASKS_CONTAINER,
    "\n".join(files),
)

cred = DefaultAzureCredential()


client = ContainerClient(storage_account_url, TASKS_CONTAINER, cred)
if len(sys.argv) >= 3:
    blob_client = BlobServiceClient.from_connection_string(sys.argv[2])
    client = blob_client.get_container_client(TASKS_CONTAINER)


with ThreadPoolExecutor() as executor:
    if not client.exists():
        log.warning("Container %s does not exist, creating it...", TASKS_CONTAINER)
        client.create_container(public_access="container")
    futures = [executor.submit(client.upload_blob, filename, data, overwrite=True) for filename, data in files.items()]
    futures.append(executor.submit(client.upload_blob, MANIFEST_FILE_NAME, dumps(hashes), overwrite=True))
    exceptions = [e for f in futures if (e := f.exception())]
    for e in exceptions:
        log.error("An error occurred while uploading a zip file", exc_info=e)
    if exceptions:
        raise SystemExit(1)

log.info("Done uploading zip files to %s/%s", storage_account_url, TASKS_CONTAINER)
