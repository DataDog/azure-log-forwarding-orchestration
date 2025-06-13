#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# Uploads files as blobs to LFO QA storage account - https://lfoqa.blob.core.windows.net
# Run from LFO root folder

set -euo pipefail

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <file_path> <container_name> <blob_name>"
    exit 1
fi

file_path=$1
container_name=$2
blob_name=$3

AZURE_TENANT_ID=$(vault kv get -field=azureTenantId kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)
AZURE_CLIENT_ID=$(vault kv get -field=azureClientId kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)
AZURE_CLIENT_SECRET=$(vault kv get -field=azureSecret kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)

az login --service-principal -u "$AZURE_CLIENT_ID" -p "$AZURE_CLIENT_SECRET" --tenant "$AZURE_TENANT_ID"

az storage container create --account-name lfoqa --auth-mode login --name "$container_name"
az storage blob upload --account-name lfoqa --auth-mode login --container-name "$container_name" --file "$file_path" --name "$blob_name" --overwrite
