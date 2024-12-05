#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <task_name> <lfo resource group>"
    echo 'Set the subscription id with `az account set --subscription <subscription id>`'
    exit 1
fi

task_name="$1"
export RESOURCE_GROUP="$2"
subscription_id=`az account show --query id -o tsv`

# Env Vars
storage_account_prefix="lfostorage"
storage_account_id=`az storage account list --resource-group $RESOURCE_GROUP --query "[?starts_with(name, '$storage_account_prefix')].id" -o tsv`
storage_account_name=`echo $storage_account_id | cut -d'/' -f 9`
export AzureWebJobsStorage=`az storage account show-connection-string --ids $storage_account_id --query connectionString -o tsv`
export MONITORED_SUBSCRIPTIONS="[\"$subscription_id\"]"
export CONTROL_PLANE_ID=${storage_account_name#"$storage_account_prefix"}
export CONTROL_PLANE_REGION=`az group show --name $RESOURCE_GROUP --query location -o tsv`

# Set default vars
if [ -z "${DD_API_KEY+x}" ]; then
    export DD_API_KEY="not_a_real_key"
fi
if [ -z "${DD_APP_KEY+x}" ]; then
    export DD_APP_KEY="not_a_real_key"
fi
if [ -z "${DD_SITE+x}" ]; then
    export DD_SITE="datadoghq.com"
fi
if [ -z "${FORWARDER_IMAGE+x}" ]; then
    export FORWARDER_IMAGE="datadoghq.azurecr.io/forwarder:latest"
fi


cd ./control_plane
python -m "tasks.$task_name"
