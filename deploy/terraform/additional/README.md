# Prerequisites Module

This Terraform module creates the necessary prerequisites for each monitored subscription in the Azure Log Forwarding Orchestration system.

## What This Module Creates

- **Resource Group**: A resource group where forwarder container apps will be deployed
- **Tags**: Standard tags for resource management and cost tracking

## Why This is Needed

The main Terraform configuration (`../complete/`) assumes that resource groups already exist in each monitored subscription. This module provides a programmatic way to create those resource groups using Terraform, rather than manual Azure CLI commands.

## Usage

### 1. Per-Subscription Deployment

This module must be applied **once per monitored subscription**. For each subscription you want to monitor:

```bash
# Navigate to the additional directory
cd deploy/terraform/additional

# Copy the example variables file
cp terraform.tfvars.example terraform.tfvars

# Edit terraform.tfvars with your subscription-specific values
# subscription_id = "your-monitored-subscription-id"
# resource_group_name = "your-resource-group-name"  # Must match main module
# location = "eastus"                               # Must match main module

# Initialize Terraform
terraform init

# Plan the deployment
terraform plan

# Apply the configuration
terraform apply
```

### 2. Multiple Subscriptions

For multiple monitored subscriptions, you can use different directories or workspaces:

#### Option A: Separate Directories
```bash
# Create separate directories for each subscription
mkdir -p subscriptions/sub1 subscriptions/sub2 subscriptions/sub3

# Copy module files to each directory
for sub in sub1 sub2 sub3; do
    cp *.tf subscriptions/$sub/
    cp terraform.tfvars.example subscriptions/$sub/terraform.tfvars
done

# Deploy to each subscription
cd subscriptions/sub1
# Edit terraform.tfvars with sub1 details
terraform init && terraform apply

cd ../sub2  
# Edit terraform.tfvars with sub2 details
terraform init && terraform apply

# And so on...
```

#### Option B: Terraform Workspaces
```bash
# Create workspaces for each subscription
terraform workspace new sub1
terraform workspace new sub2
terraform workspace new sub3

# Switch to each workspace and apply
terraform workspace select sub1
# Update terraform.tfvars with sub1 details
terraform apply

terraform workspace select sub2
# Update terraform.tfvars with sub2 details  
terraform apply
```

## Requirements

### Service Principal Permissions

The service principal running this Terraform must have the following permissions on each target subscription:

- **Contributor**: To create resource groups
- **User Access Administrator**: To manage role assignments (if extending this module)

### Consistent Configuration

The values for `resource_group_name` and `location` **must match exactly** with the main Terraform configuration:

```hcl
# This module's variables
resource_group_name = "datadog-log-forwarders"
location           = "eastus"

# Must match main module's variables
# ../complete/terraform.tfvars:
# resource_group_name = "datadog-log-forwarders"  # ✅ Same name
# location           = "eastus"                    # ✅ Same location
```

## Variables

| Variable | Type | Required | Description |
|----------|------|----------|-------------|
| `subscription_id` | string | Yes | Azure subscription ID where resources will be created |
| `resource_group_name` | string | Yes | Name of the resource group (must match main module) |
| `location` | string | Yes | Azure region (must match main module) |
| `tags` | map(string) | No | Additional tags to apply to resources |

## Outputs

| Output | Description |
|--------|-------------|
| `resource_group_name` | Name of the created resource group |
| `resource_group_id` | Full resource ID of the created resource group |
| `resource_group_location` | Location of the created resource group |
| `subscription_id` | Subscription ID where the resource group was created |

## Validation

After applying this module, you can validate the prerequisites are ready:

```bash
# Verify resource group exists
az group show \
    --name "your-resource-group-name" \
    --subscription "your-subscription-id"

# List all resource groups created by this module
az group list \
    --subscription "your-subscription-id" \
    --tag "ManagedBy=terraform" \
    --tag "Component=forwarder-prerequisites"
```

## Next Steps

After applying this module to all monitored subscriptions:

1. **Verify all prerequisites**: Ensure resource groups exist in all monitored subscriptions
2. **Deploy main module**: Run the main Terraform configuration (`../complete/`)
3. **Monitor deployment**: Check that the control plane can successfully deploy forwarders

## Troubleshooting

### Resource Group Already Exists
If the resource group already exists, Terraform will import it and manage it going forward. This is safe and expected behavior.

### Permission Denied
Ensure your service principal has Contributor permissions on the target subscription:

```bash
az role assignment create \
    --role "Contributor" \
    --assignee "your-service-principal-id" \
    --scope "/subscriptions/your-subscription-id"
```

### Inconsistent Configuration
If you get errors about missing resource groups when deploying the main module, double-check that:
- Resource group names match exactly between this module and the main module
- Locations match exactly between this module and the main module
- All monitored subscriptions have had this prerequisites module applied