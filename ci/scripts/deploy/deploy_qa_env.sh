#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.


set -euo pipefail

# setup variables
AZURE_TENANT_ID=$(vault kv get -field=azureTenantId kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)
AZURE_CLIENT_ID=$(vault kv get -field=azureClientId kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)
AZURE_CLIENT_SECRET=$(vault kv get -field=azureSecret kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)
AZURE_SUBSCRIPTION_ID=$(vault kv get -field=subscriptionId kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)
DD_API_KEY=$(vault kv get -field=ddApiKey kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)

# login to azure with app registration
az login --service-principal -u "$AZURE_CLIENT_ID" -p "$AZURE_CLIENT_SECRET" --tenant "$AZURE_TENANT_ID"

resource_group=lfoqaenv

# Clone integrations-management to get the latest templates
git clone "https://github.com/DataDog/integrations-management.git" /tmp/integrations-management
BICEP_DIR=/tmp/integrations-management/azure/logging_install/bicep

: deploy to resource group $resource_group
echo "Deploying $resource_group, view progress at https://portal.azure.com/#view/HubsExtension/DeploymentDetailsBlade/~/overview/id/%2Fproviders%2FMicrosoft.Management%2FmanagementGroups%2FAzure-Integrations-Mg%2Fproviders%2FMicrosoft.Resources%2Fdeployments%2F$resource_group"
az deployment mg create --management-group-id "Azure-Integrations-Mg" \
    --location eastus --name $resource_group --template-file "$BICEP_DIR/azuredeploy.bicep" \
    --parameters monitoredSubscriptions="[\"$AZURE_SUBSCRIPTION_ID\"]" --parameters controlPlaneLocation=eastus \
    --parameters controlPlaneSubscriptionId="$AZURE_SUBSCRIPTION_ID" --parameters controlPlaneResourceGroupName=$resource_group \
    --parameters datadogApiKey="$DD_API_KEY" --parameters datadogSite=datadoghq.com --parameters datadogTelemetry=true \
    --parameters imageRegistry=lfoqa.azurecr.io --parameters 'storageAccountUrl=https://lfoqa.blob.core.windows.net'

# Grant the deployer's managed identity Storage Blob Data Reader on the lfoqa storage account
# so it can read task zips and manifest (needed because allowBlobPublicAccess is false)
deployer_principal_id=$(az containerapp job list --resource-group $resource_group --query "[?contains(name,'deployer-task')].identity.principalId | [0]" -o tsv)
az role assignment create \
    --assignee "$deployer_principal_id" \
    --role "Storage Blob Data Reader" \
    --scope "/subscriptions/$AZURE_SUBSCRIPTION_ID/resourceGroups/lfo-qa/providers/Microsoft.Storage/storageAccounts/lfoqa"
