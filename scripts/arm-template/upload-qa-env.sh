#!/usr/bin/env bash

set -euo pipefail

az login --service-principal -u "$AZURE_CLIENT_ID" -p "$AZURE_CLIENT_SECRET" --tenant "$AZURE_TENANT_ID"

# connection=$(az storage account show-connection-string --resource-group lfo-qa --name lfoqa --query connectionString)

# az storage blob upload --container-name templates --file ./createUiDefinition.json --name createUiDefinition.json --connection-string $connection --overwrite
# az storage blob upload --container-name templates --file ./build/azuredeploy.json --name azuredeploy.json --connection-string $connection --overwrite