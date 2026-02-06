# Azure Log Forwarding Orchestration - Prerequisites

## Overview

The Terraform configuration for the Azure Log Forwarding Orchestration system requires resource groups to exist in each monitored subscription before deployment. Unlike the ARM/Bicep templates which can create these resource groups automatically using management group scope deployment, Terraform requires them to be created manually or through a separate process.

This document provides instructions for creating the necessary resource groups and ensuring the control plane has the required permissions to function correctly. You can use either the **Terraform module** (recommended) or **manual Azure CLI commands**.

## Why This is Needed

The control plane function apps need to:
- Deploy forwarder container apps in each monitored subscription
- Manage diagnostic settings for resources across subscriptions
- Scale forwarder deployments based on log volume
- Access storage accounts for log processing

Without the prerequisite resource groups, the control plane cannot deploy forwarder infrastructure in the monitored subscriptions.

## Prerequisites

Before running the Terraform configuration, you must:

1. **Create resource groups** in each monitored subscription
2. **Verify permissions** for the service principal running Terraform
3. **Ensure subscription access** for the control plane identities

## How To

### Option 1: Terraform Module (Recommended)

Use the dedicated Terraform module to create prerequisites programmatically:

```bash
# Navigate to the prerequisites module
cd ../additional

# For each monitored subscription, create the resource group:
# 1. Copy the example variables file
cp terraform.tfvars.example terraform.tfvars

# 2. Edit terraform.tfvars with your subscription details:
# subscription_id = "your-monitored-subscription-id"
# resource_group_name = "your-resource-group-name"  # Must match main module
# location = "eastus"                               # Must match main module

# 3. Apply the Terraform
terraform init
terraform plan
terraform apply

# 4. Repeat for each monitored subscription (use separate directories or workspaces)
```

For detailed instructions on using the Terraform module, see: [`../additional/README.md`](../additional/README.md)

### Option 2: Manual Azure CLI

### 1. Create Resource Groups

Create a resource group with the same name in each monitored subscription:

```bash
# Set variables
RESOURCE_GROUP_NAME="your-resource-group-name"  # Must match terraform var.resource_group_name
LOCATION="eastus"                               # Must match terraform var.location
MONITORED_SUBSCRIPTIONS=("sub-id-1" "sub-id-2" "sub-id-3")  # List of subscription IDs

# Create resource group in each subscription
for SUB_ID in "${MONITORED_SUBSCRIPTIONS[@]}"; do
    echo "Creating resource group in subscription: $SUB_ID"
    az group create \
        --name "$RESOURCE_GROUP_NAME" \
        --location "$LOCATION" \
        --subscription "$SUB_ID"
done
```

### 2. Verify Resource Group Creation

Confirm the resource groups were created successfully:

```bash
# Verify resource groups exist
for SUB_ID in "${MONITORED_SUBSCRIPTIONS[@]}"; do
    echo "Verifying resource group in subscription: $SUB_ID"
    az group show \
        --name "$RESOURCE_GROUP_NAME" \
        --subscription "$SUB_ID" \
        --query "name" \
        --output tsv
done
```

### 3. Service Principal Permissions

Ensure the service principal running Terraform has the necessary permissions:

```bash
# Get your service principal object ID
SERVICE_PRINCIPAL_ID=$(az ad sp show --id <your-app-id> --query "id" --output tsv)

# Grant permissions on each monitored subscription
for SUB_ID in "${MONITORED_SUBSCRIPTIONS[@]}"; do
    echo "Granting permissions in subscription: $SUB_ID"
    
    # Grant User Access Administrator role (needed to assign roles to managed identities)
    az role assignment create \
        --role "User Access Administrator" \
        --assignee "$SERVICE_PRINCIPAL_ID" \
        --scope "/subscriptions/$SUB_ID"
        
    # Grant Contributor role (needed to create resources)
    az role assignment create \
        --role "Contributor" \
        --assignee "$SERVICE_PRINCIPAL_ID" \
        --scope "/subscriptions/$SUB_ID"
done
```

### 4. Validate Prerequisites

Run this validation script before deploying Terraform:

```bash
#!/bin/bash
# validate-prerequisites.sh

RESOURCE_GROUP_NAME="your-resource-group-name"
MONITORED_SUBSCRIPTIONS=("sub-id-1" "sub-id-2" "sub-id-3")

echo "Validating prerequisites..."

# Check resource groups
for SUB_ID in "${MONITORED_SUBSCRIPTIONS[@]}"; do
    echo "Checking resource group in subscription: $SUB_ID"
    if az group show --name "$RESOURCE_GROUP_NAME" --subscription "$SUB_ID" &>/dev/null; then
        echo "✅ Resource group exists in $SUB_ID"
    else
        echo "❌ Resource group missing in $SUB_ID"
        exit 1
    fi
done

# Check subscription access
for SUB_ID in "${MONITORED_SUBSCRIPTIONS[@]}"; do
    echo "Checking subscription access: $SUB_ID"
    if az account show --subscription "$SUB_ID" &>/dev/null; then
        echo "✅ Can access subscription $SUB_ID"
    else
        echo "❌ Cannot access subscription $SUB_ID"
        exit 1
    fi
done

echo "🎉 All prerequisites validated successfully!"
```

### 5. Alternative: PowerShell Script

For Windows environments, use PowerShell:

```powershell
# Set variables
$ResourceGroupName = "your-resource-group-name"
$Location = "eastus"
$MonitoredSubscriptions = @("sub-id-1", "sub-id-2", "sub-id-3")

# Create resource groups
foreach ($SubId in $MonitoredSubscriptions) {
    Write-Host "Creating resource group in subscription: $SubId"
    az group create --name $ResourceGroupName --location $Location --subscription $SubId
}

# Verify creation
foreach ($SubId in $MonitoredSubscriptions) {
    Write-Host "Verifying resource group in subscription: $SubId"
    $result = az group show --name $ResourceGroupName --subscription $SubId --query "name" --output tsv
    if ($result -eq $ResourceGroupName) {
        Write-Host "✅ Resource group exists in $SubId"
    } else {
        Write-Host "❌ Resource group missing in $SubId"
    }
}
```

## Terraform Variables

Ensure your Terraform variables match the prerequisites:

```hcl
# terraform.tfvars
resource_group_name = "your-resource-group-name"  # Must match the RG name created above
location = "eastus"                               # Must match the location used above
monitored_subscriptions = [                      # Must match the subscription IDs
  "sub-id-1",
  "sub-id-2", 
  "sub-id-3"
]
```

## Common Issues

### Resource Group Name Mismatch
**Error**: `The specified resource group does not exist`
**Solution**: Ensure `var.resource_group_name` in Terraform matches the created resource group names exactly.

### Insufficient Permissions
**Error**: `AuthorizationFailed: The client does not have authorization to perform action`
**Solution**: Ensure the service principal has both Contributor and User Access Administrator roles on monitored subscriptions.

### Cross-Subscription Access
**Error**: `SubscriptionNotFound: The subscription was not found`
**Solution**: Verify the service principal has access to all monitored subscriptions and subscription IDs are correct.

## References and Additional Resources

- [Azure Resource Groups Documentation](https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/manage-resource-groups-portal)
- [Azure CLI Resource Group Commands](https://docs.microsoft.com/en-us/cli/azure/group)
- [Azure RBAC Documentation](https://docs.microsoft.com/en-us/azure/role-based-access-control/overview)
- [Terraform AzureRM Provider Documentation](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs)
- [Azure Service Principal Authentication](https://docs.microsoft.com/en-us/azure/developer/terraform/authenticate-to-azure)

## Next Steps

After completing these prerequisites:

1. Run `terraform plan` to verify the configuration
2. Run `terraform apply` to deploy the control plane
3. Monitor the deployment logs for any issues
4. Verify the control plane function apps are running correctly

For troubleshooting deployment issues, refer to the main README.md file.