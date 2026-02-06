# Azure Log Forwarding Orchestration - Complete Control Plane

This Terraform configuration deploys a complete control plane for Azure Log Forwarding Orchestration, including the Resources Task Function App and all supporting infrastructure.

## Overview

The Resources Task is a critical component of the Azure Log Forwarding Orchestration system. It:

- Discovers and catalogs resources across monitored Azure subscriptions
- Applies resource tag filters to determine which resources should have log forwarding enabled
- Maintains a cache of discovered resources for efficient processing
- Runs on a scheduled basis to keep the resource inventory up-to-date
- Provides the foundation for the scaling and diagnostic settings tasks

## Architecture

This configuration deploys:

- **Azure Function App**: Linux-based Python 3.11 function app running the resources task
- **App Service Plan**: Consumption plan for cost-effective, serverless execution
- **Storage Account**: Stores function app content and resource cache
- **Application Insights**: Monitoring and telemetry (optional, auto-created if not provided)
- **Log Analytics Workspace**: Log storage and analysis
- **Role Assignments**: Monitoring Reader permissions across monitored subscriptions
- **Managed Identity**: System-assigned identity for secure Azure resource access

## Prerequisites

1. **Azure CLI** - Install and configure
2. **Terraform** - Version 1.0 or later
3. **Azure Permissions** - See [Permissions Requirements](#permissions-requirements) below
4. **Datadog API Key** - Valid 32-character API key from your Datadog account

### Permissions Requirements

#### For Management Group Discovery (Recommended):
- `Management Group Reader` role on the target management group
- `Contributor` role on the control plane subscription/resource group
- `Monitoring Contributor` role on each monitored subscription (for diagnostic settings)
- `Monitoring Reader` role on each monitored subscription (for resource discovery)

#### For Manual Subscription List:
- `Contributor` role on the control plane subscription/resource group
- `Monitoring Contributor` role on each monitored subscription
- `Monitoring Reader` role on each monitored subscription

## Quick Start

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd azure-log-forwarding-orchestration/deploy/terraform/complete
   ```

2. **Copy and customize the variables file**
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   ```

3. **Edit terraform.tfvars** with your specific values:
   ```hcl
   resource_group_name = "my-datadog-log-forwarding-rg"
   datadog_api_key    = "your-32-character-datadog-api-key"
   
   # Option 1: Management Group Discovery (Recommended)
   use_management_group_discovery = true
   management_group_name         = "my-management-group"
   
   # Option 2: Manual Subscription List
   # use_management_group_discovery = false
   # monitored_subscriptions = ["subscription-id-1", "subscription-id-2"]
   monitored_subscriptions = ["12345678-1234-1234-1234-123456789012"]
   location               = "East US"
   datadog_site          = "datadoghq.com"
   ```

4. **Initialize Terraform**
   ```bash
   terraform init
   ```

5. **Plan the deployment**
   ```bash
   terraform plan
   ```

6. **Apply the configuration**
   ```bash
   terraform apply
   ```

## Management Group Discovery

This Terraform configuration supports automatic subscription discovery from Azure Management Groups, matching the functionality of the ARM template deployment. This is the **recommended approach** for enterprise environments.

### Benefits of Management Group Discovery

- **Automatic subscription discovery**: No need to manually maintain subscription lists
- **Dynamic scaling**: New subscriptions added to the management group are automatically included
- **Centralized governance**: Leverage existing management group structure
- **ARM template compatibility**: Matches the behavior of the bicep deployment
- **Reduced maintenance**: Eliminates manual subscription list updates

### Configuration Examples

#### Basic Management Group Discovery
```hcl
use_management_group_discovery = true
management_group_name         = "production-workloads"
```

#### With Excluded Subscriptions
```hcl
use_management_group_discovery = true
management_group_name         = "production-workloads"
excluded_subscriptions        = [
  "11111111-1111-1111-1111-111111111111",  # Dev/test subscription
  "22222222-2222-2222-2222-222222222222"   # Sandbox subscription
]
```

#### Manual Subscription List (Fallback)
```hcl
use_management_group_discovery = false
monitored_subscriptions = [
  "12345678-1234-1234-1234-123456789012",
  "87654321-4321-4321-4321-210987654321"
]
```

### Required Permissions for Management Groups

To use management group discovery, ensure the Terraform execution context has:

1. **Management Group Reader** role on the target management group
2. **Subscription Reader** role on subscriptions within the management group (usually inherited)

### Resource Group Management

When `create_resource_groups_in_subscriptions = true`, the system will create forwarder resource groups in each monitored subscription. This matches the ARM template behavior where resource groups are created in each subscription for forwarder deployments.

**Note**: Cross-subscription resource group creation in Terraform requires appropriate permissions on all target subscriptions.

## Configuration

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `resource_group_name` | Target resource group name | `"datadog-log-forwarding-rg"` |
| `datadog_api_key` | Datadog API key (32 chars) | `"abcd1234..."` |
| `monitored_subscriptions` | List of subscription IDs to monitor | `["sub-id-1", "sub-id-2"]` |

### Common Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `location` | Azure region | `"East US"` |
| `datadog_site` | Datadog site | `"datadoghq.com"` |
| `datadog_telemetry` | Enable telemetry | `false` |
| `log_level` | Application log level | `"INFO"` |
| `resource_tag_filters` | Tag filters | `""` |
| `resources_task_schedule` | Cron schedule | `"0 */5 * * * *"` |
| `cache_retention_days` | Cache retention period | `7` |

### Resource Tag Filters

Filter resources by tags to control which resources have log forwarding enabled:

```hcl
resource_tag_filters = "env:prod,datadog:true,!env:test"
```

- **Inclusion**: `env:prod` includes resources tagged with `env:prod`
- **Exclusion**: `!env:test` excludes resources tagged with `env:test`
- **Wildcards**: `env:prod*` matches `env:production`, `env:prod-v2`, etc.

### Monitoring and Alerting

```hcl
enable_monitoring      = true
alert_email_addresses = ["ops-team@company.com"]
```

### Security Configuration

```hcl
ip_restrictions = [
  {
    ip_address = "192.168.1.0/24"
    name       = "Office Network"
    priority   = 100
    action     = "Allow"
  }
]
```

## Deployment Examples

### Development Environment

```hcl
# terraform.tfvars
resource_group_name     = "datadog-log-forwarding-dev"
datadog_api_key        = "your-dev-api-key"
monitored_subscriptions = ["dev-subscription-id"]
location               = "East US"
log_level              = "DEBUG"
datadog_telemetry      = false
cache_retention_days   = 3

tags = {
  Environment = "development"
  Project     = "datadog-log-forwarding"
  Owner       = "dev-team"
}
```

### Production Environment

```hcl
# terraform.tfvars
resource_group_name     = "datadog-log-forwarding-prod"
datadog_api_key        = "your-prod-api-key"
monitored_subscriptions = ["prod-sub-1", "prod-sub-2"]
location               = "East US"
log_level              = "INFO"
datadog_telemetry      = true
cache_retention_days   = 14
storage_account_replication_type = "GRS"

tags = {
  Environment = "production"
  Project     = "datadog-log-forwarding"
  Owner       = "platform-team"
  CriticalSystem = "true"
}
```

## Outputs

After deployment, useful outputs include:

- `control_plane_id` - Unique identifier for this control plane
- `resources_task_function_app_name` - Name of the function app
- `resources_task_principal_id` - Managed identity principal ID
- `storage_account_name` - Storage account name
- `application_insights_connection_string` - Monitoring connection string

## Monitoring

### Application Insights

Monitor function app performance and errors:

```bash
# View function app logs
az monitor app-insights query \
  --app <app-insights-name> \
  --analytics-query "traces | where cloud_RoleName == 'resources-task-<control-plane-id>'"
```

### Function App Logs

```bash
# Stream logs
az webapp log tail --name <function-app-name> --resource-group <resource-group>
```

### Storage Account Monitoring

Monitor cache storage usage:

```bash
# List cache blobs
az storage blob list \
  --account-name <storage-account-name> \
  --container-name control-plane-cache
```

## Troubleshooting

### Common Issues

1. **Permission Errors**
   - Ensure the deployment account has Contributor access to the resource group
   - Verify the managed identity has Monitoring Reader on target subscriptions

2. **Function App Not Starting**
   - Check Application Insights for startup errors
   - Verify all required environment variables are set
   - Ensure Python dependencies are compatible

3. **Storage Account Issues**
   - Verify storage account name is globally unique
   - Check firewall rules and network access
   - Ensure blob container exists and is accessible

### Debugging Commands

```bash
# Check function app status
az functionapp show --name <function-app-name> --resource-group <resource-group>

# Test function app connectivity
az functionapp function show --name <function-app-name> --resource-group <resource-group> --function-name resources_task_timer

# Check role assignments
az role assignment list --assignee <principal-id> --scope "/subscriptions/<subscription-id>"
```

## Maintenance

### Updates

1. **Update Terraform Configuration**
   ```bash
   terraform plan
   terraform apply
   ```

2. **Update Function App Code**
   Deploy new function app code separately using Azure DevOps, GitHub Actions, or Azure CLI

3. **Scale Resources**
   The consumption plan auto-scales, but you can monitor usage through Application Insights

### Backup and Recovery

- **Function App**: Code should be stored in source control
- **Storage Account**: Enable geo-redundancy for production
- **Configuration**: Store terraform.tfvars in secure location

## Integration with Other Components

This Resources Task is designed to work with:

- **Scaling Task**: Uses resource cache to create and manage log forwarders
- **Diagnostic Settings Task**: Uses resource assignments to configure diagnostic settings
- **Deployer Task**: Manages deployment of other control plane components

## Security Considerations

- **Managed Identity**: Uses system-assigned managed identity for secure Azure access
- **API Keys**: Datadog API key is marked as sensitive and stored securely
- **Network Security**: Configure IP restrictions and firewall rules as needed
- **RBAC**: Minimum required permissions (Monitoring Reader) are assigned

## Cost Optimization

- **Consumption Plan**: Pay-per-execution model for cost efficiency
- **Storage Lifecycle**: Automatic cleanup of old cache data
- **Monitoring**: Use Application Insights sampling to reduce costs
- **Resource Tagging**: Comprehensive tagging for cost allocation

## Support

For issues and questions:

1. Check the troubleshooting section above
2. Review Azure Function App logs and Application Insights
3. Consult the main project documentation
4. Open an issue in the project repository

## License

This project is licensed under the Apache-2.0 License. See the LICENSE file for details. 