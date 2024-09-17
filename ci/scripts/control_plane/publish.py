#!/usr/bin/env python3

# stdlib
from concurrent.futures import ThreadPoolExecutor
from os import listdir

# 3p
from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient

ACCOUNT_URL = "https://ddazurelfo.blob.core.windows.net"
TASKS_CONTAINER = "tasks"


zips: dict[str, bytes] = {}
for file in listdir("dist"):
    if file.endswith(".zip"):
        with open(f"dist/{file}", "rb") as z:
            zips[file] = z.read()

print(
    f"Uploading the following zip files to {ACCOUNT_URL}/{TASKS_CONTAINER}\n",
    "\n".join(zips.keys()),
)

with (
    DefaultAzureCredential() as cred,
    ContainerClient(ACCOUNT_URL, TASKS_CONTAINER, cred) as client,
    ThreadPoolExecutor() as executor,
):
    for zip, data in zips.items():
        executor.submit(client.upload_blob, zip, data, overwrite=True)


print(f"Done uploading zip files to {ACCOUNT_URL}/{TASKS_CONTAINER}")
