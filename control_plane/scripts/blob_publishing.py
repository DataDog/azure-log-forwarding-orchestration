# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

from logging import WARNING, getLogger

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

from cache.manifest_cache import PUBLIC_STORAGE_ACCOUNT_URL

getLogger("azure").setLevel(WARNING)


def get_container_client(storage_account_url: str, container: str, connection_string: str | None = None) -> ContainerClient:
    if connection_string is not None:
        return BlobServiceClient.from_connection_string(connection_string).get_container_client(container)
    return ContainerClient(storage_account_url, container, DefaultAzureCredential())


def ensure_container_exists(client: ContainerClient, storage_account_url: str) -> None:
    log = getLogger(__name__)
    if not client.exists():
        log.warning("Container %s does not exist, creating it...", client.container_name)
        if storage_account_url == PUBLIC_STORAGE_ACCOUNT_URL:
            client.create_container(public_access="container")
        else:
            client.create_container()
