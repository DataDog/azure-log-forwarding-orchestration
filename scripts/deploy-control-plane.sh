#!/usr/bin/env bash

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

subscription_id=$(az account show --query id --output tsv)

cd ./control_plane
cache_name=$(python -c "from cache.common import BLOB_STORAGE_CACHE; print(BLOB_STORAGE_CACHE, end='')")
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
az storage container list --account-name $storage_account --auth-mode login | jq -r '.[].name' | grep $cache_name >/dev/null || {
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

declare -A task_roles
task_roles[resources_task]="Monitoring Reader"
task_roles[diagnostic_settings_task]="Monitoring Contributor"
task_roles[scaling_task]="Contributor"

get-scope() {
    if [[ $1 == "Contributor" ]]; then
        echo "/subscriptions/$subscription_id/resourceGroups/$resource_group"
    else
        echo "/subscriptions/$subscription_id"
    fi
}

for task in "${!task_roles[@]}"; do
    if [[ ! -d "./dist/$task" ]]; then
        echo "Task $task has not been built, skipping."
        continue
    fi
    function_app_name="${task//_/-}"
    role="${task_roles[$task]}"

    # Create the function app if it doesn't exist
    [[ "$existing_functions" != *"$function_app_name"* ]] && {
        echo -n "Function app $function_app_name does not exist, creating one..."
        az functionapp create --resource-group $resource_group --plan $app_service_plan --name $function_app_name --storage-account $storage_account \
            --runtime python --runtime-version 3.11 --functions-version 4 --os-type Linux
        echo Done.
    }

    # Deploying function app code
    cd ./dist/$task
    echo Deploying $function_app_name...
    while true; do
        func azure functionapp publish $function_app_name --python && break
        # needed because sometimes it takes a few seconds for the function app to exist
        echo "Failed to deploy $function_app_name, retrying in 5 seconds..."
        echo "Press Ctrl+C to cancel."
        sleep 5
    done
    cd ../..

    echo -n Checking permissions for $function_app_name...
    principal_id=$(az functionapp identity show --name $function_app_name --resource-group $resource_group --query principalId --output tsv)
    [ -z "$principal_id" ] && {
        echo -n Enabling managed identity for $function_app_name...
        principal_id=$(az functionapp identity assign --name $function_app_name --resource-group $resource_group | jq -r .principalId)
    }
    echo Done.


    echo -n Checking role assignment for $function_app_name...
    set +e
    while true; do
        role_assignments="$(az role assignment list --assignee $principal_id --query "[].roleDefinitionName" --output tsv)"
        [ $? -eq 0 ] && break
        # needed because sometimes it takes a few seconds for the principal id to exist
        echo "Failed to list role assignments for $function_app_name, retrying in 5 seconds..."
        echo "Press Ctrl+C to cancel."
        sleep 5
    done
    set -e
    [[ $role_assignments != *"$role"* ]] && {
        echo -n "$role role not found for $task. Assigning role..."
        scope=$(get-scope "$role")
        az role assignment create --assignee $principal_id --role "$role" --scope $scope
    }
    echo Done.

    echo -n Checking RESOURCE_GROUP setting for $function_app_name...

    rg_setting="$(az functionapp config appsettings list --name $function_app_name --resource-group lfo --query "[?name=='RESOURCE_GROUP']" | jq -r '.[].value')"
    [[ "$rg_setting" != "$resource_group" ]] && {
        echo -n "Setting RESOURCE_GROUP for $function_app_name..."
        az functionapp config appsettings set --name $function_app_name --resource-group $resource_group --settings RESOURCE_GROUP=$resource_group 2> /dev/null > /dev/null
    }
    echo Done.
done

echo All Done!
