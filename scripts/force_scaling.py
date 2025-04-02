#!/usr/bin/env python
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

import json
import os
import random
import sys

from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobServiceClient

float_max = sys.float_info.max
big_int = sys.maxsize / 100

if len(sys.argv) != 3:
    print("Usage: python force_scaling.py <resource-group> <location>")
    sys.exit(1)


def list_blobs_flat(blob_service_client: BlobServiceClient, container_name):
    container_client = blob_service_client.get_container_client(container=container_name)
    blob_list = container_client.list_blobs()
    return blob_list


def download_blob(blob_service_client: BlobServiceClient, blob_name: str) -> list:
    blob_client = blob_service_client.get_blob_client(container="dd-forwarder", blob=blob_name)
    output = []
    # encoding param is necessary for readall() to return str, otherwise it returns bytes
    downloader = blob_client.download_blob(max_concurrency=1, encoding="UTF-8")
    content = downloader.readall()
    for line in content.split("\n"):
        if line:
            output.append(json.loads(line))
    return output


def upload_blob_data(blob_service_client: BlobServiceClient, blob_name: str, data: list):
    content = ""
    for item in data:
        content += json.dumps(item) + "\n"
    blob_client = blob_service_client.get_blob_client(container="dd-forwarder", blob=blob_name)
    # Upload the blob data - default blob type is BlockBlob
    blob_client.upload_blob(content.encode("utf-8"), blob_type="BlockBlob", overwrite=True)


def update_metrics(metrics: list):
    last_metric = metrics[-1]
    resources = [i for i in last_metric.get("resource_log_volume", {})]
    if len(resources) < 2:
        print("Need at least two resources generating logs.")
        sys.exit(1)
    problem_resource = random.choice(resources)
    print(f"Choosing resource {problem_resource} as resource generating load.")
    last_metric["runtime_seconds"] = float_max
    last_metric["resource_log_volume"][problem_resource] = big_int
    last_metric["resource_log_bytes"][problem_resource] = big_int * 10
    return [last_metric.copy() for _ in range(100)]


credential = DefaultAzureCredential()
# Retrieve subscription ID from environment variable.
subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]

# Obtain the management object for resources.
resource_client = ResourceManagementClient(credential, subscription_id)

resource_group = sys.argv[1]
location = sys.argv[2]

resource_list = resource_client.resources.list_by_resource_group(resource_group, expand="createdTime,changedTime")

resources = [i for i in resource_list if i.location == location and i.name and "ddlogstorage" in i.name]
if not resources:
    print(f"No storage account found in resource group {resource_group} in location {location}")
    sys.exit(1)

storage_account = random.choice(resources)
print(f"Running against storage account {storage_account.name}")
account_url = f"https://{storage_account.name}.blob.core.windows.net"

storage_client = StorageManagementClient(credential, subscription_id)

keys = storage_client.storage_accounts.list_keys(resource_group, storage_account.name)
connection_string = f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName={storage_account.name};AccountKey={keys.keys[0].value}"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)


newest_blob = None
for blob in list_blobs_flat(blob_service_client, "dd-forwarder"):
    if "metrics_" not in blob.name:
        continue
    if not newest_blob or blob.creation_time > newest_blob.creation_time:
        newest_blob = blob

if not newest_blob:
    print(f"No metrics blobs found in storage account {storage_account.name}")
    sys.exit(1)

print(f"Downloading blob {newest_blob.name}")
metrics = download_blob(blob_service_client, newest_blob.name)
metrics = update_metrics(metrics)
upload_blob_data(blob_service_client, newest_blob.name, metrics)
print(f"Updated metrics in blob {newest_blob.name}")
