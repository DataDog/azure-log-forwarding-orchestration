#!/usr/bin/env python3


from concurrent.futures import ThreadPoolExecutor
from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient
from tasks.__main__ import TASKS

ACCOUNT_URL = "https://ddazurelfo.blob.core.windows.net"
TASKS_CONTAINER = "tasks"


for task in TASKS:
    print(task)

raise SystemExit(0)

with DefaultAzureCredential() as cred, ContainerClient(
    ACCOUNT_URL, TASKS_CONTAINER, cred
) as client, ThreadPoolExecutor() as executor:
    client.upload_blob("hello.txt", b"Hello, Azure!")
    for container in client.list_containers():
        print(container.name)
