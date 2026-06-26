#!/usr/bin/env python3
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# usage: publish_images.py <public_storage_account_url> <registry> <version_tag>

# stdlib
import sys
from json import dumps
from logging import INFO, WARNING, basicConfig, getLogger

# 3p
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

from cache.manifest_cache import (
    PUBLIC_STORAGE_ACCOUNT_URL,
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
getLogger("azure").setLevel(WARNING)

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

cred = DefaultAzureCredential()
client = ContainerClient(storage_account_url, TASKS_CONTAINER, cred)
if len(sys.argv) >= 5:
    blob_client = BlobServiceClient.from_connection_string(sys.argv[4])
    client = blob_client.get_container_client(TASKS_CONTAINER)

if not client.exists():
    log.warning("Container %s does not exist, creating it...", TASKS_CONTAINER)
    if storage_account_url == PUBLIC_STORAGE_ACCOUNT_URL:
        client.create_container(public_access="container")
    else:
        client.create_container()

client.upload_blob(TASK_IMAGES_MANIFEST_FILE_NAME, dumps(manifest), overwrite=True)
log.info("Done publishing task images manifest to %s/%s", storage_account_url, TASKS_CONTAINER)
