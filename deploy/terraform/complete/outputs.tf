# Control Plane Outputs
output "control_plane_id" {
  description = "The unique identifier for the control plane"
  value       = local.control_plane_id
}

output "resource_group_name" {
  description = "Name of the resource group"
  value       = data.azurerm_resource_group.current.name
}

output "location" {
  description = "Azure region where resources are deployed"
  value       = var.location
}

# Storage Account Outputs
output "storage_account_name" {
  description = "Name of the control plane storage account"
  value       = azurerm_storage_account.control_plane_storage.name
}

output "storage_account_id" {
  description = "ID of the control plane storage account"
  value       = azurerm_storage_account.control_plane_storage.id
}

output "storage_account_primary_access_key" {
  description = "Primary access key of the control plane storage account"
  value       = azurerm_storage_account.control_plane_storage.primary_access_key
  sensitive   = true
}

output "storage_connection_string" {
  description = "Connection string for the control plane storage account"
  value       = local.storage_connection_string
  sensitive   = true
}

# Function App Outputs
output "resources_task_function_app_name" {
  description = "Name of the resources task function app"
  value       = azurerm_linux_function_app.resources_task.name
}

output "resources_task_function_app_id" {
  description = "ID of the resources task function app"
  value       = azurerm_linux_function_app.resources_task.id
}

output "resources_task_function_app_url" {
  description = "Default hostname of the resources task function app"
  value       = azurerm_linux_function_app.resources_task.default_hostname
}

output "resources_task_principal_id" {
  description = "Principal ID of the resources task function app managed identity"
  value       = azurerm_linux_function_app.resources_task.identity[0].principal_id
}

output "resources_task_tenant_id" {
  description = "Tenant ID of the resources task function app managed identity"
  value       = azurerm_linux_function_app.resources_task.identity[0].tenant_id
}

output "scaling_task_function_app_name" {
  description = "Name of the scaling task function app"
  value       = azurerm_linux_function_app.scaling_task.name
}

output "scaling_task_function_app_id" {
  description = "ID of the scaling task function app"
  value       = azurerm_linux_function_app.scaling_task.id
}

output "scaling_task_function_app_url" {
  description = "Default hostname of the scaling task function app"
  value       = azurerm_linux_function_app.scaling_task.default_hostname
}

output "scaling_task_principal_id" {
  description = "Principal ID of the scaling task function app managed identity"
  value       = azurerm_linux_function_app.scaling_task.identity[0].principal_id
}

output "scaling_task_tenant_id" {
  description = "Tenant ID of the scaling task function app managed identity"
  value       = azurerm_linux_function_app.scaling_task.identity[0].tenant_id
}

output "diagnostic_settings_task_function_app_name" {
  description = "Name of the diagnostic settings task function app"
  value       = azurerm_linux_function_app.diagnostic_settings_task.name
}

output "diagnostic_settings_task_function_app_id" {
  description = "ID of the diagnostic settings task function app"
  value       = azurerm_linux_function_app.diagnostic_settings_task.id
}

output "diagnostic_settings_task_function_app_url" {
  description = "Default hostname of the diagnostic settings task function app"
  value       = azurerm_linux_function_app.diagnostic_settings_task.default_hostname
}

output "diagnostic_settings_task_principal_id" {
  description = "Principal ID of the diagnostic settings task function app managed identity"
  value       = azurerm_linux_function_app.diagnostic_settings_task.identity[0].principal_id
}

output "diagnostic_settings_task_tenant_id" {
  description = "Tenant ID of the diagnostic settings task function app managed identity"
  value       = azurerm_linux_function_app.diagnostic_settings_task.identity[0].tenant_id
}

# App Service Plan Outputs
output "app_service_plan_name" {
  description = "Name of the app service plan"
  value       = azurerm_service_plan.control_plane_asp.name
}

output "app_service_plan_id" {
  description = "ID of the app service plan"
  value       = azurerm_service_plan.control_plane_asp.id
}

# Application Insights Outputs
# Removed to match ARM/Bicep parity

# Log Analytics Outputs
# Removed to match ARM/Bicep parity

# Cache Container Outputs
output "cache_container_name" {
  description = "Name of the cache container"
  value       = azurerm_storage_container.cache_container.name
}

# Configuration Outputs
output "monitored_subscriptions" {
  description = "List of monitored subscription IDs"
  value       = var.monitored_subscriptions
}

output "datadog_site" {
  description = "Datadog site configuration"
  value       = var.datadog_site
}

output "resource_tag_filters" {
  description = "Resource tag filters configuration"
  value       = var.resource_tag_filters
}

output "log_level" {
  description = "Log level configuration"
  value       = var.log_level
}

output "resources_task_schedule" {
  description = "Resources task schedule configuration"
  value       = var.resources_task_schedule
}

# Container App Environment Outputs
output "container_app_environment_name" {
  description = "Name of the container app environment"
  value       = azurerm_container_app_environment.deployer_env.name
}

output "container_app_environment_id" {
  description = "ID of the container app environment"
  value       = azurerm_container_app_environment.deployer_env.id
}

# Deployer Task Container App Job Outputs
output "deployer_task_name" {
  description = "Name of the deployer task container app job"
  value       = azurerm_container_app_job.deployer_task.name
}

output "deployer_task_id" {
  description = "ID of the deployer task container app job"
  value       = azurerm_container_app_job.deployer_task.id
}

output "deployer_task_principal_id" {
  description = "Principal ID of the deployer task container app job managed identity"
  value       = azurerm_container_app_job.deployer_task.identity[0].principal_id
}

output "deployer_task_tenant_id" {
  description = "Tenant ID of the deployer task container app job managed identity"
  value       = azurerm_container_app_job.deployer_task.identity[0].tenant_id
}


# Role Assignment Outputs
output "role_assignments" {
  description = "Map of subscription IDs to role assignment IDs"
  value = {
    resources_task_monitoring_reader                = { for k, v in azurerm_role_assignment.resources_task_subscription_monitoring_reader : k => v.id }
    scaling_task_contributor                        = { for k, v in azurerm_role_assignment.scaling_task_rg_contributor : k => v.id }
    diagnostic_settings_task_monitoring_contributor = { for k, v in azurerm_role_assignment.diagnostic_settings_task_subscription_monitoring_contributor : k => v.id }
    diagnostic_settings_task_storage_access         = { for k, v in azurerm_role_assignment.diagnostic_settings_task_rg_storage_access : k => v.id }
    deployer_task_website_contributor               = azurerm_role_assignment.deployer_task_website_contributor.id
  }
}

# Deployment Information
output "deployment_timestamp" {
  description = "Timestamp of the deployment"
  value       = timestamp()
}

output "terraform_version" {
  description = "Version of Terraform used for deployment"
  value       = ">=1.0"
}

# Connection Information for Integration
output "function_app_connection_info" {
  description = "Connection information for the function app"
  value = {
    app_name         = azurerm_linux_function_app.resources_task.name
    resource_group   = data.azurerm_resource_group.current.name
    subscription_id  = data.azurerm_client_config.current.subscription_id
    tenant_id        = data.azurerm_client_config.current.tenant_id
    principal_id     = azurerm_linux_function_app.resources_task.identity[0].principal_id
    default_hostname = azurerm_linux_function_app.resources_task.default_hostname
  }
}

# Storage Information for Integration
output "storage_connection_info" {
  description = "Storage connection information"
  value = {
    storage_account_name = azurerm_storage_account.control_plane_storage.name
    cache_container_name = azurerm_storage_container.cache_container.name
    file_share_name      = azurerm_storage_share.control_plane_fileshare.name
  }
} 