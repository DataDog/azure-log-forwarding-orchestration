#!/usr/bin/env bash

set -euo pipefail

AZURE_TENANT_ID=$(vault kv get -field=azureTenantId kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)
AZURE_CLIENT_ID=$(vault kv get -field=azureClientId kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)
AZURE_CLIENT_SECRET=$(vault kv get -field=azureSecret kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)

az login --service-principal -u "$AZURE_CLIENT_ID" -p "$AZURE_CLIENT_SECRET" --tenant "$AZURE_TENANT_ID"

export connection=$(az storage account show-connection-string --resource-group lfo-qa --name lfoqa --query connectionString)

az storage blob upload --container-name templates --file ./createUiDefinition.json --name createUiDefinition.json --connection-string $connection --overwrite
az storage blob upload --container-name templates --file ./build/azuredeploy.json --name azuredeploy.json --connection-string $connection --overwrite