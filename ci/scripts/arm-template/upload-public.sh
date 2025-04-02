#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# Uploads files to blobs in public-facing storage account - https://ddazurelfo.core.blob.windows.net
# Run from LFO root folder

set -euxo pipefail

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <file_path> <container_name> <blob_name>"
    exit 1
fi

file_path=$1
container_name=$2
blob_name=$3

az login --identity
az storage container create --name "$container_name" --account-name ddazurelfo --auth-mode login --public-access blob
az storage blob upload --account-name ddazurelfo --auth-mode login --container-name "$container_name" --file "$file_path" --name "$blob_name" --overwrite
