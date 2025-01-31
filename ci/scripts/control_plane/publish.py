#!/usr/bin/env python3
# usage: publish.py <public_storage_account_url> [connection_string]

# stdlib
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from json import dumps
from logging import WARNING, getLogger, basicConfig, INFO
import sys

# 3p
from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient, BlobServiceClient

from cache.manifest_cache import (
    ManifestCache,
    ALL_ZIPS,
    RESOURCES_TASK_ZIP,
    SCALING_TASK_ZIP,
    DIAGNOSTIC_SETTINGS_TASK_ZIP,
    FORWARDER_ZIP,
    MANIFEST_FILE_NAME,
    TASKS_CONTAINER,
)

if len(sys.argv) < 2:
    print("Usage: publish.py <public_storage_account_url>")
    sys.exit(1)

storage_account_url = sys.argv[1]

basicConfig(level=INFO)
log = getLogger("publish")
getLogger("azure").setLevel(WARNING)

log.info("Reading zip files from dist/")
zips: dict[str, bytes] = {}
for file in ALL_ZIPS:
    with open(f"dist/{file}", "rb") as z:
        zips[file] = z.read()

log.info("Generating Hashes for the zip files")
hashes: ManifestCache = {
    "resources": sha256(zips[RESOURCES_TASK_ZIP]).hexdigest(),
    "scaling": sha256(zips[SCALING_TASK_ZIP]).hexdigest(),
    "diagnostic_settings": sha256(zips[DIAGNOSTIC_SETTINGS_TASK_ZIP]).hexdigest(),
    "forwarder": sha256(zips[FORWARDER_ZIP]).hexdigest(),
}

log.info(
    "Uploading the following zip files to %s/%s:\n%s",
    storage_account_url,
    TASKS_CONTAINER,
    "\n".join(zips),
)

cred = DefaultAzureCredential()


client = ContainerClient(storage_account_url, TASKS_CONTAINER, cred)
if len(sys.argv) >= 3:
    blob_client = BlobServiceClient.from_connection_string(sys.argv[2])
    client = blob_client.get_container_client(TASKS_CONTAINER)


with (
    ThreadPoolExecutor() as executor,
):
    if not client.exists():
        log.warning("Container %s does not exist, creating it...", TASKS_CONTAINER)
        client.create_container(public_access="container")
    futures = [executor.submit(client.upload_blob, zip, data, overwrite=True) for zip, data in zips.items()]
    futures.append(executor.submit(client.upload_blob, MANIFEST_FILE_NAME, dumps(hashes), overwrite=True))
    exceptions = [e for f in futures if (e := f.exception())]
    for e in exceptions:
        log.error("An error occurred while uploading a zip file", exc_info=e)
    if exceptions:
        raise SystemExit(1)

log.info("Done uploading zip files to %s/%s", storage_account_url, TASKS_CONTAINER)
