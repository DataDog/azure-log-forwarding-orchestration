from logging import getLogger
from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient, PublicAccess

from cache.manifest_cache import (
    PUBLIC_STORAGE_ACCOUNT_URL,
    TASKS_CONTAINER,
)

log = getLogger(__name__)


with (
    DefaultAzureCredential() as cred,
    ContainerClient(PUBLIC_STORAGE_ACCOUNT_URL, TASKS_CONTAINER, cred) as client,
):
    client.set_container_access_policy(
        signed_identifiers={}, public_access=PublicAccess.BLOB
    )

log.info("Container %s is now public", TASKS_CONTAINER)
