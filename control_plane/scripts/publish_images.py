#!/usr/bin/env python3
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# usage: publish_images.py <public_storage_account_url> <registry> <version_tag>

# stdlib
import sys
from json import dumps
from logging import INFO, basicConfig, getLogger

from blob_publishing import ensure_container_exists, get_container_client

from cache.manifest_cache import (
    TASK_IMAGES_MANIFEST_FILE_NAME,
    TASKS_CONTAINER,
    ManifestCache,
)

if len(sys.argv) < 4:
    print("Usage: publish_images.py <public_storage_account_url> <registry> <version_tag>")
    sys.exit(1)

storage_account_url = sys.argv[1]
registry = sys.argv[2]
version_tag = sys.argv[3]

basicConfig(level=INFO)
log = getLogger("publish_images")

manifest: ManifestCache = {
    "resources": f"{registry}/resources-task:{version_tag}",
    "scaling": f"{registry}/scaling-task:{version_tag}",
    "diagnostic_settings": f"{registry}/diagnostic-settings-task:{version_tag}",
}

log.info(
    "Publishing task images manifest to %s/%s/%s:\n%s",
    storage_account_url,
    TASKS_CONTAINER,
    TASK_IMAGES_MANIFEST_FILE_NAME,
    manifest,
)

connection_string = sys.argv[4] if len(sys.argv) >= 5 else None
client = get_container_client(storage_account_url, TASKS_CONTAINER, connection_string)
ensure_container_exists(client, storage_account_url)

client.upload_blob(TASK_IMAGES_MANIFEST_FILE_NAME, dumps(manifest), overwrite=True)
log.info("Done publishing task images manifest to %s/%s", storage_account_url, TASKS_CONTAINER)
