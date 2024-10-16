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
tasks=$(python -m tasks)
cd ..

echo -n "Checking for current function apps..."
existing_functions="$(az functionapp list -g $resource_group | jq -r '.[].name')"
echo Done.

for task in $tasks; do
    if [[ ! -d "./dist/$task" ]]; then
        echo "Task $task has not been built, skipping."
        continue
    fi
    function_app_name=$((grep "${task//_/-}" <<<"$existing_functions") | cut -d$'\n' -f1)

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

done

echo All Done!
