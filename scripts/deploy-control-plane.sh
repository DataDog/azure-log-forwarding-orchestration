#!/bin/bash

set -eo pipefail

# install deps
command -v jq >/dev/null 2>&1 || {
    echo installing jq...
    brew install jq
}

command -v az >/dev/null 2>&1 || {
    echo installing azure-cli and logging in...
    brew install azure-cli
    az login
}

command -v func >/dev/null 2>&1 || {
    echo installing azure-functions-core-tools...
    brew tap azure/functions
    brew install brew install azure-functions-core-tools@4
}

if [ -z "$1" ]; then
    echo "Usage: $0 <resource_group>"
    exit 1
fi
resource_group=$1

set -u

cd ./control_plane
tasks="$(python -m tasks)"
cache_name=`python -c "from cache.common import BLOB_STORAGE_CACHE; print(BLOB_STORAGE_CACHE, end='')"`
cd ..

random_id=$((RANDOM % 9000 + 1000))

# ================ creating dependency resources ================

echo -n "Checking for current function apps..."
existing_functions="$(az functionapp list -g $resource_group | jq -r '.[].name')"
echo Done.

echo -n "Checking for a storage account..."
storage_account="$(az storage account list -g $resource_group | jq -r '.[].name' | cut -d$'\n' -f1)"
if [[ -z "$storage_account" ]]; then
    echo "Storage account does not exist, creating one..."
    storage_account="lfo$random_id"
    az storage account create --name $storage_account --resource-group $resource_group --location eastus --sku Standard_LRS
fi
echo Done.

echo -n "Checking for storage containers..."
az storage container list --account-name $storage_account --auth-mode login | jq -r '.[].name' | grep $cache_name > /dev/null || {
    echo -n "Missing Cache Container, creating..."
    az storage container create --name $cache_name --account-name $storage_account
}
echo Done.

echo -n "Checking for an app service plan..."
app_service_plan="$(az functionapp plan list -g $resource_group | jq -r '.[].name' | cut -d$'\n' -f1)"
if [[ -z "$app_service_plan" ]]; then
    echo "app service plan does not exist, creating one..."
    app_service_plan="ASPlfo$random_id"
    az functionapp plan create --name $app_service_plan --resource-group $resource_group --location eastus --sku EP1 --is-linux
fi
echo Done.


# ================ creating control plane function apps ================

for task in $tasks; do
    function_app_name="${task//_/-}"
    if [[ "$existing_functions" != *"$function_app_name"* ]]; then
        echo -n "Function app $function_app_name does not exist, creating one..."
        az functionapp create --resource-group $resource_group --plan $app_service_plan --name $function_app_name --storage-account $storage_account \
            --runtime python --runtime-version 3.11 --functions-version 4 --os-type Linux
        echo Done.
    fi
    if [[ ! -d "./dist/$task" ]]; then
        echo "Task $task has not been built, skipping."
        continue
    fi
    cd ./dist/$task
    echo Deploying $function_app_name...
    while true; do
        func azure functionapp publish $function_app_name --python && break
        echo "Failed to deploy $function_app_name, retrying in 5 seconds..."
        echo "Press Ctrl+C to cancel."
        sleep 5
    done
    cd ../..
done

# ================ set up permissions ================

# ================ resource task permissions ================
cd ./control_plane
subscription_id=$(az account show --query id --output tsv)
# Get the principal ID of the Function App's managed identity
echo -n Checking permissions for resource task...
principal_id=$(az functionapp identity show --name resources-task --resource-group $resource_group --query principalId --output tsv)
[ -z "$principal_id" ] && {
    echo -n Enabling managed identity for resources-task...
    principal_id=$(az functionapp identity assign --name resources-task --resource-group $resource_group | jq -r .principalId)
}
# Check if the "Monitoring Reader" role is already assigned
role_assignments=$(az role assignment list --assignee $principal_id --query "[].roleDefinitionName" --output tsv)
if [[ $role_assignments != *"Monitoring Reader"* ]]; then
    echo -n "Monitoring Reader role not found for resources-task. Assigning role..."
    az role assignment create --assignee $principal_id --role "Monitoring Reader" --scope "/subscriptions/$subscription_id"
fi


# ================ diagnostic settings task permissions ================

echo -n Checking permissions for diagnostic settings task...
principal_id=$(az functionapp identity show --name diagnostic-settings-task --resource-group $resource_group --query principalId --output tsv)
[ -z "$principal_id" ] && {
    echo -n Enabling managed identity for diagnostic-settings-task...
    principal_id=$(az functionapp identity assign --name diagnostic-settings-task --resource-group $resource_group | jq -r .principalId)
}
role_assignments=$(az role assignment list --assignee $principal_id --query "[].roleDefinitionName" --output tsv)
if [[ $role_assignments != *"Monitoring Contributor"* ]]; then
    echo -n "Monitoring Reader role not found for diagnostic-settings-task. Assigning role..."
    az role assignment create --assignee $principal_id --role "Monitoring Contributor" --scope "/subscriptions/$subscription_id"
fi

echo Done.


echo All Done!
