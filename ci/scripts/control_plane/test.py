#!/usr/bin/env python

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

account_url = "https://ddazurelfo.blob.core.windows.net"
credential = DefaultAzureCredential()

blob_service_client = BlobServiceClient(account_url, credential=credential)
containers = blob_service_client.list_containers()

for container in containers:
    print(container.name)
