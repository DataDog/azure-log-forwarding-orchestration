#!/usr/bin/env python3
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# usage: publish.py <public_storage_account_url> [connection_string]

# stdlib
import sys
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from itertools import chain
from json import dumps
from logging import INFO, basicConfig, getLogger

from blob_publishing import ensure_container_exists, get_container_client

from cache.manifest_cache import (
    ALL_ZIPS,
    DIAGNOSTIC_SETTINGS_TASK_ZIP,
    RESOURCES_TASK_ZIP,
    SCALING_TASK_ZIP,
    TASK_ZIPS_MANIFEST_FILE_NAME,
    TASKS_CONTAINER,
    ManifestCache,
)

if len(sys.argv) < 2:
    print("Usage: publish.py <public_storage_account_url>")
    sys.exit(1)

storage_account_url = sys.argv[1]

basicConfig(level=INFO)
log = getLogger("publish")

log.info("Reading artifacts from dist/")
files: dict[str, bytes] = {}
for filename in chain(ALL_ZIPS, ["initial_run.sh"]):
    with open(f"dist/{filename}", "rb") as f:
        files[filename] = f.read()

log.info("Generating Hashes for the files")
hashes: ManifestCache = {
    "resources": sha256(files[RESOURCES_TASK_ZIP]).hexdigest(),
    "scaling": sha256(files[SCALING_TASK_ZIP]).hexdigest(),
    "diagnostic_settings": sha256(files[DIAGNOSTIC_SETTINGS_TASK_ZIP]).hexdigest(),
}

log.info(
    "Uploading the following files to %s/%s:\n%s",
    storage_account_url,
    TASKS_CONTAINER,
    "\n".join(files),
)

connection_string = sys.argv[2] if len(sys.argv) >= 3 else None
client = get_container_client(storage_account_url, TASKS_CONTAINER, connection_string)

with ThreadPoolExecutor() as executor:
    # The public storage account needs public container access, but storage accounts
    # created for personal environments can't have public access.
    ensure_container_exists(client, storage_account_url)
    futures = [executor.submit(client.upload_blob, filename, data, overwrite=True) for filename, data in files.items()]
    futures.append(executor.submit(client.upload_blob, TASK_ZIPS_MANIFEST_FILE_NAME, dumps(hashes), overwrite=True))
    exceptions = [e for f in futures if (e := f.exception())]
    for e in exceptions:
        log.error("An error occurred while uploading a file", exc_info=e)
    if exceptions:
        raise SystemExit(1)

log.info("Done uploading files to %s/%s", storage_account_url, TASKS_CONTAINER)
