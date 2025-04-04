#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <task_name> <lfo resource group>"
    echo 'Set the subscription id with `az account set --subscription <subscription id>`'
    exit 1
fi

task_name="$1"
export RESOURCE_GROUP="$2"
export SUBSCRIPTION_ID=$(az account show --query id -o tsv)
echo Using Subscription ID: "$SUBSCRIPTION_ID"

# Env Vars
echo Looking for LFO Storage Account in "$RESOURCE_GROUP"
storage_account_prefix="lfostorage"
storage_account_id=$(az storage account list --resource-group "$RESOURCE_GROUP" --query "[?starts_with(name, '$storage_account_prefix')].id" -o tsv)
echo Using Storage Account: "$storage_account_id"
storage_account_name=$(cut -d'/' -f 9 <<<$storage_account_id)
export AzureWebJobsStorage=$(az storage account show-connection-string --ids $storage_account_id --query connectionString -o tsv)
echo Using Storage Connection String for "$storage_account_name"
export MONITORED_SUBSCRIPTIONS="[\"$SUBSCRIPTION_ID\"]"
export CONTROL_PLANE_ID=${storage_account_name#"$storage_account_prefix"}
export CONTROL_PLANE_REGION=$(az group show --name $RESOURCE_GROUP --query location -o tsv)
echo Control Plane Region: "$CONTROL_PLANE_REGION"
export REGION=$CONTROL_PLANE_REGION
export STORAGE_ACCOUNT_URL='https://ddazurelfo.blob.core.windows.net'

# Set default vars
if [ -z "${DD_API_KEY+x}" ]; then
    export DD_API_KEY="not_a_real_key"
fi
if [ -z "${DD_SITE+x}" ]; then
    export DD_SITE="datadoghq.com"
fi
if [ -z "${DD_TELEMETRY+x}" ]; then
    export DD_TELEMETRY="true"
fi
if [ -z "${LOG_LEVEL+x}" ]; then
    export LOG_LEVEL="DEBUG"
fi
if [ -z "${FORWARDER_IMAGE+x}" ]; then
    export FORWARDER_IMAGE="datadoghq.azurecr.io/forwarder:latest"
fi

cd ./control_plane
echo "Running task $task_name"
python -m "tasks.$task_name"
