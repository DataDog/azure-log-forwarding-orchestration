#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <resource_group>"
    exit 1
fi
set -euxo pipefail
resource_group=$1

cd ./control_plane
tasks="$(python -m tasks)"
cd -

existing_functions="$(az functionapp list -g $resource_group | jq -r '.[].name')"
storage_account="$(az storage account list -g $resource_group | jq -r '.[].name' | cut -d$'\n' -f1)"

if [[ -z "$storage_account" ]]; then
    echo "Storage account does not exist, creating one"
    az storage account create --name lfo1234 --resource-group $resource_group --location eastus --sku Standard_LRS
    storage_account="lfo1234"
fi

for task in $tasks; do
    function_app_name="${task//_/-}"
    if [[ "$existing_functions" != *"$function_app_name"* ]]; then
        echo "Function app $function_app_name does not exist"
        az functionapp create --resource-group $resource_group --consumption-plan-location eastus --name $function_app_name --storage-account $storage_account --runtime custom --functions-version 4 --os-type Linux
        echo "Function app $function_app_name created"
    fi
    cd ./dist/$task
    func azure functionapp publish $function_app_name --custom
    cd -
done
