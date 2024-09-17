#!/usr/bin/env python3

# stdlib
from concurrent.futures import ThreadPoolExecutor
from os import listdir
from logging import WARNING, getLogger, basicConfig, INFO

# 3p
from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient

ACCOUNT_URL = "https://ddazurelfo.blob.core.windows.net"
TASKS_CONTAINER = "tasks"

basicConfig(level=INFO)
log = getLogger("publish")
getLogger("azure").setLevel(WARNING)

log.info("Reading zip files from dist/")
zips: dict[str, bytes] = {}
for file in listdir("dist"):
    if file.endswith(".zip"):
        with open(f"dist/{file}", "rb") as z:
            zips[file] = z.read()

log.info(
    "Uploading the following zip files to %s/%s\n%s",
    ACCOUNT_URL,
    TASKS_CONTAINER,
    "\n".join(zips.keys()),
)

with (
    DefaultAzureCredential() as cred,
    ContainerClient(ACCOUNT_URL, TASKS_CONTAINER, cred) as client,
    ThreadPoolExecutor() as executor,
):
    if not client.exists():
        log.warning("Container %s does not exist, creating it...", TASKS_CONTAINER)
        client.create_container()
    futures = [
        executor.submit(client.upload_blob, zip, data, overwrite=True)
        for zip, data in zips.items()
    ]
    exceptions = [e for f in futures if (e := f.exception())]
    for e in exceptions:
        log.error("An error occurred while uploading a zip file", exc_info=e)
    if exceptions:
        raise SystemExit(1)

log.info("Done uploading zip files to %s/%s", ACCOUNT_URL, TASKS_CONTAINER)
