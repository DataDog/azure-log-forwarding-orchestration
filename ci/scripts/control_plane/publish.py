#!/usr/bin/env python3

# stdlib
from concurrent.futures import ThreadPoolExecutor
from os import listdir
from os.path import isfile

# 3p
from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient

ACCOUNT_URL = "https://ddazurelfo.blob.core.windows.net"
TASKS_CONTAINER = "tasks"

zips = [f for f in listdir("dist") if isfile(f) and f.endswith(".zip")]

print(zips)
raise SystemExit(0)

with DefaultAzureCredential() as cred, ContainerClient(
    ACCOUNT_URL, TASKS_CONTAINER, cred
) as client, ThreadPoolExecutor() as executor:
    client.upload_blob("hello.txt", b"Hello, Azure!")
    for container in client.list_containers():
        print(container.name)
