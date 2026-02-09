# Azure to Terraform Migration Guide: Datadog Log Forwarding Orchestration

This document provides a comprehensive guide for migrating the Azure Log Forwarding Orchestration system from Bicep to Terraform. It includes resource mappings, deployment patterns, and best practices based on analysis of the current Bicep templates.

## Table of Contents

1. [Overview](#overview)
2. [Azure Resource to Terraform Equivalents](#azure-resource-to-terraform-equivalents)
3. [Multi-Subscription Deployment Patterns](#multi-subscription-deployment-patterns)
4. [Azure Built-in Roles Reference](#azure-built-in-roles-reference)
5. [Deployment Script Alternatives](#deployment-script-alternatives)
6. [Migration Challenges and Considerations](#migration-challenges-and-considerations)
7. [Best Practices](#best-practices)

## Overview

The current Azure Log Forwarding Orchestration system uses 8 Bicep modules to deploy resources across multiple Azure subscriptions at management group scope. The system includes:

- **Control Plane**: Function Apps, Container Apps, and Storage in a central subscription
- **Forwarder Jobs**: Container Apps deployed across monitored subscriptions
- **IAM Configuration**: Built-in and custom role assignments
- **Validation Scripts**: Azure CLI-based validation for configuration

This guide provides Terraform equivalents for all components and patterns for replicating the multi-subscription deployment architecture.

## Azure Resource to Terraform Equivalents

Based on analysis of the current Bicep templates, here are the Terraform equivalents for all Azure resources used:

### Resource Mapping Table

| Azure Resource Type | Bicep Usage | Terraform Resource | Support Status | Key Differences |
|---------------------|-------------|-------------------|---------------|-----------------|
| `Microsoft.Web/serverfarms` | App Service Plans for Function Apps | `azurerm_service_plan` | ✅ Full Support | Use `os_type` and `sku_name` parameters |
| `Microsoft.Storage/storageAccounts` | Storage for logs and cache | `azurerm_storage_account` | ✅ Full Support | Direct equivalent |
| `Microsoft.Storage/storageAccounts/fileServices` | File services for Function Apps | ⚠️ Auto-created | ⚠️ Implicit | Cannot explicitly configure |
| `Microsoft.Storage/storageAccounts/blobServices` | Blob services for log storage | ⚠️ Auto-created | ⚠️ Implicit | Cannot explicitly configure |
| `Microsoft.Storage/storageAccounts/blobServices/containers` | Log storage containers | `azurerm_storage_container` | ✅ Full Support | Use `storage_account_id` parameter |
| `Microsoft.Storage/storageAccounts/managementPolicies` | Lifecycle management | `azurerm_storage_management_policy` | ✅ Full Support | Direct equivalent |
| `Microsoft.Web/sites` | Function Apps for tasks | `azurerm_linux_function_app` | ✅ Full Support | Use Linux-specific resource |
| `Microsoft.App/managedEnvironments` | Container Apps Environment | `azurerm_container_app_environment` | ✅ Full Support | Direct equivalent |
| `Microsoft.App/jobs` | Container Apps Jobs | `azurerm_container_app_job` | ✅ Full Support | Direct equivalent |
| `Microsoft.ManagedIdentity/userAssignedIdentities` | Managed identities | `azurerm_user_assigned_identity` | ✅ Full Support | Direct equivalent |
| `Microsoft.Authorization/roleDefinitions` | Custom roles | `azurerm_role_definition` | ✅ Full Support | Direct equivalent |
| `Microsoft.Authorization/roleAssignments` | Role assignments | `azurerm_role_assignment` | ✅ Full Support | Direct equivalent |
| `Microsoft.Resources/deploymentScripts` | Validation scripts | ❌ No equivalent | ❌ Not supported | See alternatives below |
| `Microsoft.Resources/resourceGroups` | Resource groups | `azurerm_resource_group` | ✅ Full Support | Direct equivalent |

### Detailed Resource Examples

#### App Service Plan (for Function Apps)
```hcl
resource "azurerm_service_plan" "control_plane_asp" {
  name                = "control-plane-asp-${var.control_plane_id}"
  location            = var.control_plane_location
  resource_group_name = azurerm_resource_group.control_plane.name
  os_type             = "Linux"
  sku_name            = "Y1"  # Dynamic consumption plan

  tags = var.tags
}
```

#### Storage Account with Lifecycle Policy
```hcl
resource "azurerm_storage_account" "lfo_storage" {
  name                     = "lfostorage${var.control_plane_id}"
  resource_group_name      = azurerm_resource_group.control_plane.name
  location                 = var.control_plane_location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  access_tier              = "Hot"
  min_tls_version          = "TLS1_2"

  tags = var.tags
}

resource "azurerm_storage_container" "cache_container" {
  name                  = "control-plane-cache"
  storage_account_id    = azurerm_storage_account.lfo_storage.id
  container_access_type = "private"
}

resource "azurerm_storage_management_policy" "lifecycle_policy" {
  storage_account_id = azurerm_storage_account.lfo_storage.id

  rule {
    name    = "delete-old-blobs"
    enabled = true
    
    filters {
      blob_types = ["blockBlob", "appendBlob"]
    }
    
    actions {
      base_blob {
        delete_after_days_since_modification_greater_than = var.storage_account_retention_days
      }
      snapshot {
        delete_after_days_since_creation_greater_than = var.storage_account_retention_days
      }
    }
  }
}
```

#### Linux Function App
```hcl
resource "azurerm_linux_function_app" "resources_task" {
  name                       = "resources-task-${var.control_plane_id}"
  location                   = var.control_plane_location
  resource_group_name        = azurerm_resource_group.control_plane.name
  service_plan_id            = azurerm_service_plan.control_plane_asp.id
  storage_account_name       = azurerm_storage_account.lfo_storage.name
  storage_account_access_key = azurerm_storage_account.lfo_storage.primary_access_key
  https_only                 = true

  site_config {
    application_stack {
      python_version = "3.11"
    }
  }

  app_settings = {
    "AzureWebJobsStorage"              = local.connection_string
    "DD_API_KEY"                       = var.datadog_api_key
    "DD_SITE"                         = var.datadog_site
    "DD_TELEMETRY"                    = var.datadog_telemetry ? "true" : "false"
    "CONTROL_PLANE_ID"                = var.control_plane_id
    "AzureWebJobsFeatureFlags"        = "EnableWorkerIndexing"
    "FUNCTIONS_EXTENSION_VERSION"     = "~4"
    "FUNCTIONS_WORKER_RUNTIME"        = "python"
    "WEBSITE_CONTENTAZUREFILECONNECTIONSTRING" = local.connection_string
    "WEBSITE_CONTENTSHARE"            = "resources-task-${var.control_plane_id}"
    "MONITORED_SUBSCRIPTIONS"         = var.monitored_subscriptions
    "RESOURCE_TAG_FILTERS"            = var.resource_tag_filters
    "LOG_LEVEL"                       = var.log_level
  }

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}
```

#### Container Apps Environment and Job
```hcl
resource "azurerm_container_app_environment" "deployer_env" {
  name                = "dd-log-forwarder-env-${var.control_plane_id}-${var.control_plane_location}"
  location            = var.control_plane_location
  resource_group_name = azurerm_resource_group.control_plane.name

  tags = var.tags
}

resource "azurerm_container_app_job" "deployer_task" {
  name                         = "deployer-task-${var.control_plane_id}"
  location                     = var.control_plane_location
  resource_group_name          = azurerm_resource_group.control_plane.name
  container_app_environment_id = azurerm_container_app_environment.deployer_env.id

  replica_timeout_in_seconds = 1800
  replica_retry_limit        = 1

  manual_trigger_config {
    parallelism              = 1
    replica_completion_count = 1
  }

  schedule_trigger_config {
    cron_expression                = "*/30 * * * *"
    parallelism                    = 1
    replica_completion_count       = 1
  }

  template {
    container {
      name   = "deployer-task-${var.control_plane_id}"
      image  = "${var.image_registry}/deployer:latest"
      cpu    = 0.5
      memory = "1Gi"

      env {
        name        = "AzureWebJobsStorage"
        secret_name = "connection-string"
      }
      env {
        name  = "SUBSCRIPTION_ID"
        value = var.control_plane_subscription_id
      }
      env {
        name  = "RESOURCE_GROUP"
        value = var.control_plane_resource_group_name
      }
      env {
        name  = "CONTROL_PLANE_ID"
        value = var.control_plane_id
      }
      env {
        name  = "CONTROL_PLANE_REGION"
        value = var.control_plane_location
      }
      env {
        name        = "DD_API_KEY"
        secret_name = "dd-api-key"
      }
      env {
        name  = "DD_SITE"
        value = var.datadog_site
      }
      env {
        name  = "DD_TELEMETRY"
        value = var.datadog_telemetry ? "true" : "false"
      }
      env {
        name  = "STORAGE_ACCOUNT_URL"
        value = var.storage_account_url
      }
      env {
        name  = "LOG_LEVEL"
        value = var.log_level
      }
    }
  }

  secret {
    name  = "connection-string"
    value = local.connection_string
  }

  secret {
    name  = "dd-api-key"
    value = var.datadog_api_key
  }

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}
```

## Multi-Subscription Deployment Patterns

The current Bicep templates deploy at management group scope across multiple subscriptions. Terraform requires different patterns to achieve similar functionality.

### Provider Configuration for Multiple Subscriptions

```hcl
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

# Control plane subscription (default provider)
provider "azurerm" {
  features {}
  subscription_id = var.control_plane_subscription_id
}

# Monitored subscriptions (using for_each with provider aliases)
provider "azurerm" {
  for_each = toset(var.monitored_subscription_ids)
  alias    = "monitored_${each.key}"
  
  features {}
  subscription_id = each.key
}
```

### Alternative Pattern: Dynamic Providers

Since Terraform doesn't support `for_each` on providers directly, use a module pattern:

```hcl
# main.tf
module "control_plane" {
  source = "./modules/control-plane"
  
  control_plane_subscription_id = var.control_plane_subscription_id
  control_plane_location        = var.control_plane_location
  control_plane_resource_group  = var.control_plane_resource_group
  # ... other variables
}

module "subscription_permissions" {
  source   = "./modules/subscription-permissions"
  for_each = toset(var.monitored_subscription_ids)
  
  providers = {
    azurerm = azurerm
  }
  
  subscription_id                         = each.key
  control_plane_id                       = module.control_plane.control_plane_id
  resource_task_principal_id             = module.control_plane.resource_task_principal_id
  diagnostic_settings_task_principal_id  = module.control_plane.diagnostic_settings_task_principal_id
  scaling_task_principal_id              = module.control_plane.scaling_task_principal_id
  initial_run_identity_principal_id      = module.control_plane.initial_run_identity_principal_id
}
```

### Management Group Alternative Patterns

Since Terraform doesn't directly support management group scope deployments like Bicep, use these alternatives:

#### Option 1: Explicit Subscription Iteration
```hcl
# Deploy to each subscription explicitly
module "forwarder_deployment" {
  source   = "./modules/forwarder"
  for_each = toset(var.monitored_subscription_ids)
  
  providers = {
    azurerm = azurerm
  }
  
  subscription_id          = each.key
  control_plane_id        = var.control_plane_id
  forwarder_image         = var.forwarder_image
  # ... other configuration
}
```

#### Option 2: Hybrid ARM Template Deployment
```hcl
# Use ARM template for management group scope operations
resource "azurerm_management_group_template_deployment" "policy_assignment" {
  name                = "lfo-policy-assignment"
  location            = var.control_plane_location
  management_group_id = var.management_group_id
  
  template_content = file("${path.module}/arm-templates/policy-assignment.json")
  
  parameters_content = jsonencode({
    controlPlaneId = {
      value = var.control_plane_id
    }
    monitoredSubscriptions = {
      value = var.monitored_subscription_ids
    }
  })
}
```

#### Option 3: AzAPI Provider for Advanced Scenarios
```hcl
terraform {
  required_providers {
    azapi = {
      source = "azure/azapi"
    }
  }
}

# Management group-level policy assignment using AzAPI
resource "azapi_resource" "management_group_policy" {
  type      = "Microsoft.Authorization/policyAssignments@2022-06-01"
  name      = "datadog-lfo-policy"
  parent_id = "/providers/Microsoft.Management/managementGroups/${var.management_group_id}"
  
  body = {
    properties = {
      policyDefinitionId = "/providers/Microsoft.Authorization/policyDefinitions/${var.policy_definition_id}"
      parameters = {
        controlPlaneId = {
          value = var.control_plane_id
        }
      }
    }
  }
}
```

### Cross-Subscription Dependencies

Handle dependencies between resources in different subscriptions:

```hcl
# Data source to reference control plane resources from other subscriptions
data "azurerm_function_app" "resources_task" {
  provider            = azurerm.control_plane
  name                = "resources-task-${var.control_plane_id}"
  resource_group_name = var.control_plane_resource_group
}

# Use the data source in role assignments
resource "azurerm_role_assignment" "monitoring_reader" {
  scope                = "/subscriptions/${var.monitored_subscription_id}"
  role_definition_name = "Monitoring Reader"
  principal_id         = data.azurerm_function_app.resources_task.identity[0].principal_id
}
```

## Azure Built-in Roles Reference

The current Bicep templates use five Azure built-in roles. Here's how to reference them in Terraform:

### Roles Used in Current Implementation

| Role Name | GUID | Usage in Bicep | Terraform Reference |
|-----------|------|----------------|-------------------|
| Monitoring Reader | `43d0d8ad-25c7-4714-9337-8ba259a9fe05` | Resource task permissions | `role_definition_name = "Monitoring Reader"` |
| Monitoring Contributor | `749f88d5-cbae-40b8-bcfc-e573ddc772fa` | Diagnostic settings permissions | `role_definition_name = "Monitoring Contributor"` |
| Contributor | `b24988ac-6180-42a0-ab88-20f7382dd24c` | Scaling task permissions | `role_definition_name = "Contributor"` |
| Website Contributor | `de139f84-1756-47ae-9be6-808fbbe84772` | Deployer task permissions | `role_definition_name = "Website Contributor"` |
| Reader and Data Access | `c12c1c16-33a1-487b-954d-41c89c60f349` | Storage access permissions | `role_definition_name = "Reader and Data Access"` |

### Role Assignment Examples

#### Subscription-Level Role Assignment
```hcl
# Resources task monitoring reader role (subscription scope)
resource "azurerm_role_assignment" "resource_task_monitoring" {
  scope                = "/subscriptions/${var.monitored_subscription_id}"
  role_definition_name = "Monitoring Reader"
  principal_id         = azurerm_linux_function_app.resources_task.identity[0].principal_id
  
  description = "ddlfo${var.control_plane_id}"
}
```

#### Resource Group-Level Role Assignment
```hcl
# Scaling task contributor role (resource group scope)
resource "azurerm_role_assignment" "scaling_task_contributor" {
  scope                = azurerm_resource_group.forwarder_rg.id
  role_definition_name = "Contributor"
  principal_id         = azurerm_linux_function_app.scaling_task.identity[0].principal_id
  
  description = "ddlfo${var.control_plane_id}"
}
```

#### Custom Role Definition and Assignment
```hcl
# Custom role for container app start permissions
resource "azurerm_role_definition" "container_app_start" {
  name  = "ContainerAppStartRole${var.control_plane_id}"
  scope = azurerm_resource_group.control_plane.id
  
  description = "Custom role to start container app jobs"
  
  permissions {
    actions = [
      "Microsoft.App/jobs/start/action"
    ]
  }
  
  assignable_scopes = [
    azurerm_resource_group.control_plane.id
  ]
}

resource "azurerm_role_assignment" "initial_run_container_app" {
  scope              = azurerm_resource_group.control_plane.id
  role_definition_id = azurerm_role_definition.container_app_start.role_definition_resource_id
  principal_id       = azurerm_user_assigned_identity.initial_run.principal_id
  principal_type     = "ServicePrincipal"
  
  description = "ddlfo${var.control_plane_id}"
}
```

### Role Assignment Best Practices

1. **Use Role Names Over GUIDs**: More readable and maintainable
```hcl
# Preferred
role_definition_name = "Monitoring Reader"

# Alternative (when needed)
role_definition_id = "/subscriptions/${var.subscription_id}/providers/Microsoft.Authorization/roleDefinitions/43d0d8ad-25c7-4714-9337-8ba259a9fe05"
```

2. **Scope at the Most Restrictive Level**:
```hcl
# Resource group scope (preferred when possible)
scope = azurerm_resource_group.example.id

# Subscription scope (when cross-resource access needed)
scope = "/subscriptions/${var.subscription_id}"
```

3. **Use Data Sources for Existing Roles**:
```hcl
data "azurerm_role_definition" "monitoring_reader" {
  name = "Monitoring Reader"
}

resource "azurerm_role_assignment" "example" {
  scope              = var.scope
  role_definition_id = data.azurerm_role_definition.monitoring_reader.id
  principal_id       = var.principal_id
}
```

## Deployment Script Alternatives

The current Bicep templates use Azure deployment scripts for validation and initialization. Terraform requires alternative approaches:

### Current Deployment Script Usage

1. **API Key Validation** (`validate_config.bicep`):
   - Validates Datadog API key against specified site
   - Validates YAML format for PII scrubber rules

2. **Initial Run** (`initial_run.bicep`):
   - Complex Python-based initialization script
   - Triggers initial resource discovery and setup

### Terraform Alternatives

#### Option 1: External Data Sources (Recommended for Validation)

```hcl
# External data source for Datadog API validation
data "external" "datadog_validation" {
  program = ["python3", "${path.module}/scripts/validate-datadog.py"]
  
  query = {
    api_key = var.datadog_api_key
    site    = var.datadog_site
  }
}

# External data source for YAML validation
data "external" "yaml_validation" {
  program = ["python3", "${path.module}/scripts/validate-yaml.py"]
  
  query = {
    yaml_content = var.pii_scrubber_rules
  }
}

# Use validation results in resources
resource "azurerm_linux_function_app" "example" {
  # ... other configuration
  
  app_settings = {
    "DD_API_KEY"          = data.external.datadog_validation.result.validated_key
    "PII_SCRUBBER_RULES" = data.external.yaml_validation.result.validated_yaml
    # ... other settings
  }
  
  depends_on = [
    data.external.datadog_validation,
    data.external.yaml_validation
  ]
}
```

#### Option 2: Terraform Validation Rules

```hcl
variable "datadog_api_key" {
  description = "Datadog API Key"
  type        = string
  sensitive   = true
  
  validation {
    condition     = length(var.datadog_api_key) == 32
    error_message = "Datadog API key must be exactly 32 characters long."
  }
}

variable "pii_scrubber_rules" {
  description = "YAML formatted list of PII Scrubber Rules"
  type        = string
  default     = ""
  
  validation {
    condition = var.pii_scrubber_rules == "" || can(yamldecode(var.pii_scrubber_rules))
    error_message = "PII scrubber rules must be valid YAML format."
  }
}
```

#### Option 3: Local Exec Provisioner (For Complex Initialization)

```hcl
resource "terraform_data" "initial_run" {
  triggers_replace = [
    var.control_plane_id,
    var.datadog_api_key,
    var.monitored_subscriptions
  ]
  
  provisioner "local-exec" {
    command = "python3 ${path.module}/scripts/initial-run.py"
    
    environment = {
      AzureWebJobsStorage    = local.connection_string
      DD_API_KEY            = var.datadog_api_key
      DD_SITE               = var.datadog_site
      DD_TELEMETRY          = var.datadog_telemetry ? "true" : "false"
      CONTROL_PLANE_ID      = var.control_plane_id
      CONTROL_PLANE_REGION  = var.control_plane_location
      RESOURCE_GROUP        = var.control_plane_resource_group_name
      SUBSCRIPTION_ID       = var.control_plane_subscription_id
      LOG_LEVEL             = var.log_level
      MONITORED_SUBSCRIPTIONS = var.monitored_subscriptions
      PII_SCRUBBER_RULES    = var.pii_scrubber_rules
      RESOURCE_TAG_FILTERS  = var.resource_tag_filters
    }
  }
  
  depends_on = [
    azurerm_storage_account.lfo_storage,
    azurerm_linux_function_app.resources_task,
    azurerm_container_app_job.deployer_task
  ]
}
```

#### Option 4: Azure Container Instance (Most Similar to Deployment Scripts)

```hcl
resource "azurerm_container_group" "initial_run" {
  name                = "initial-run-${var.control_plane_id}"
  location            = var.control_plane_location
  resource_group_name = azurerm_resource_group.control_plane.name
  os_type             = "Linux"
  restart_policy      = "Never"
  
  container {
    name   = "initial-run"
    image  = "${var.image_registry}/initial-run:latest"
    cpu    = "0.5"
    memory = "1.0"
    
    environment_variables = {
      CONTROL_PLANE_ID      = var.control_plane_id
      CONTROL_PLANE_REGION  = var.control_plane_location
      RESOURCE_GROUP        = var.control_plane_resource_group_name
      SUBSCRIPTION_ID       = var.control_plane_subscription_id
      LOG_LEVEL             = var.log_level
      MONITORED_SUBSCRIPTIONS = var.monitored_subscriptions
      RESOURCE_TAG_FILTERS  = var.resource_tag_filters
    }
    
    secure_environment_variables = {
      AzureWebJobsStorage = local.connection_string
      DD_API_KEY         = var.datadog_api_key
      PII_SCRUBBER_RULES = var.pii_scrubber_rules
    }
  }
  
  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.initial_run.id]
  }
  
  depends_on = [
    azurerm_storage_account.lfo_storage,
    azurerm_linux_function_app.resources_task,
    azurerm_container_app_job.deployer_task
  ]
}
```

### Migration Recommendations

1. **For Validation**: Use external data sources or Terraform validation rules
2. **For Simple Scripts**: Use `local-exec` provisioner with `terraform_data`
3. **For Complex Initialization**: Use Azure Container Instances
4. **For Security**: Avoid provisioners when possible; prefer Azure Container Instances

## Migration Challenges and Considerations

### Key Differences Between Bicep and Terraform

1. **Deployment Scope**:
   - **Bicep**: Native management group scope support
   - **Terraform**: Requires explicit subscription iteration or hybrid approaches

2. **State Management**:
   - **Bicep**: Stateless, declarative deployments
   - **Terraform**: Stateful, requires careful state management across subscriptions

3. **Resource Dependencies**:
   - **Bicep**: Automatic dependency resolution within templates
   - **Terraform**: Explicit dependency management across modules/providers

4. **Script Execution**:
   - **Bicep**: Native deployment script support
   - **Terraform**: Requires alternative patterns (provisioners, external scripts)

### Migration Strategy

1. **Phase 1**: Convert individual Bicep modules to Terraform modules
2. **Phase 2**: Implement multi-subscription deployment pattern
3. **Phase 3**: Replace deployment scripts with Terraform alternatives
4. **Phase 4**: Optimize state management and CI/CD integration

### Testing Approach

1. **Unit Testing**: Test individual Terraform modules
2. **Integration Testing**: Test cross-subscription dependencies
3. **Validation Testing**: Verify deployment script alternatives
4. **End-to-End Testing**: Complete system deployment validation

## Best Practices

### Terraform Structure

```
terraform/
├── modules/
│   ├── control-plane/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   └── versions.tf
│   ├── subscription-permissions/
│   └── forwarder/
├── environments/
│   ├── dev/
│   ├── staging/
│   └── prod/
├── scripts/
│   ├── validate-datadog.py
│   ├── validate-yaml.py
│   └── initial-run.py
└── main.tf
```

### Security Considerations

1. **Sensitive Variables**: Use Terraform sensitive variables for API keys
2. **State Security**: Use encrypted remote state storage
3. **Role Assignments**: Follow principle of least privilege
4. **Secrets Management**: Use Azure Key Vault for secret storage

### Performance Optimization

1. **Parallel Execution**: Use modules to enable parallel resource creation
2. **State Locking**: Implement proper state locking for team environments
3. **Resource Targeting**: Use `-target` for selective deployments during development

This guide provides a comprehensive foundation for migrating from Bicep to Terraform while maintaining the functionality and security of the Azure Log Forwarding Orchestration system.