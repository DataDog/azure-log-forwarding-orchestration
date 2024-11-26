#!/usr/bin/env bash
# Uploads LFO ARM template files as blobs to LFO QA storage account - https://lfoqa.blob.core.windows.net

set -euo pipefail

AZURE_TENANT_ID=$(vault kv get -field=azureTenantId kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)
AZURE_CLIENT_ID=$(vault kv get -field=azureClientId kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)
AZURE_CLIENT_SECRET=$(vault kv get -field=azureSecret kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)

az login --service-principal -u "$AZURE_CLIENT_ID" -p "$AZURE_CLIENT_SECRET" --tenant "$AZURE_TENANT_ID"

az storage blob upload --account-name lfoqa --auth-mode login --container-name templates --file ./createUiDefinition.json --name createUiDefinition.json --overwrite
az storage blob upload --account-name lfoqa --auth-mode login --container-name templates --file ./build/azuredeploy.json --name azuredeploy.json --overwrite