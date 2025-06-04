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
DD_APP_KEY=$(vault kv get -field=ddAppKey kv/k8s/gitlab-runner/azure-log-forwarding-orchestration/qa)

# login to azure with app registration
az login --service-principal -u "$AZURE_CLIENT_ID" -p "$AZURE_CLIENT_SECRET" --tenant "$AZURE_TENANT_ID"

resource_group=lfoqaenv

: deploy to resource group $resource_group
echo "Deploying $resource_group, view progress at https://portal.azure.com/#view/HubsExtension/DeploymentDetailsBlade/~/overview/id/%2Fproviders%2FMicrosoft.Management%2FmanagementGroups%2FAzure-Integrations-Mg%2Fproviders%2FMicrosoft.Resources%2Fdeployments%2F$resource_group"
az deployment mg create --management-group-id "Azure-Integrations-Mg" \
    --location eastus --name $resource_group --template-file ./deploy/azuredeploy.bicep \
    --parameters monitoredSubscriptions="[\"$AZURE_SUBSCRIPTION_ID\"]" --parameters controlPlaneLocation=eastus \
    --parameters controlPlaneSubscriptionId="$AZURE_SUBSCRIPTION_ID" --parameters controlPlaneResourceGroupName=$resource_group \
    --parameters datadogApiKey="$DD_API_KEY" --parameters datadogSite=datadoghq.com --parameters datadogTelemetry=true \
    --parameters datadogApplicationKey="$DD_APP_KEY" --parameters \
    --parameters imageRegistry=lfoqa.azurecr.io --parameters 'storageAccountUrl=https://lfoqa.blob.core.windows.net'
