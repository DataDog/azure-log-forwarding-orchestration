#!/usr/bin/env bash

set -euxo pipefail

# setup variables
AZURE_TENANT_ID=$(vault kv get -field=azureTenantId kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)
AZURE_CLIENT_ID=$(vault kv get -field=azureClientId kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)
AZURE_CLIENT_SECRET=$(vault kv get -field=azureSecret kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)
AZURE_SUBSCRIPTION_ID=$(vault kv get -field=subscriptionId kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)
DD_API_KEY=$(vault kv get -field=ddApiKey kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)
DD_APP_KEY=$(vault kv get -field=ddAppKey kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)

# login to azure with app registration
az login --service-principal -u "$AZURE_CLIENT_ID" -p "$AZURE_CLIENT_SECRET" --tenant "$AZURE_TENANT_ID"

# deploy to resource group lfoqaenv
az deployment mg create --management-group-id "Azure-Integrations-Mg" --location eastus --name lfoqaenv --template-file ./deploy/azuredeploy.bicep --parameters monitoredSubscriptions="$AZURE_SUBSCRIPTION_ID" --parameters controlPlaneLocation=eastus --parameters controlPlaneSubscriptionId="$AZURE_SUBSCRIPTION_ID" --parameters controlPlaneResourceGroupName=lfoqaenv --parameters datadogApplicationKey="$DD_APP_KEY" --parameters datadogApiKey="$DD_API_KEY" --parameters datadogSite=datadoghq.com --parameters imageRegistry=lfoqa.azurecr.io
