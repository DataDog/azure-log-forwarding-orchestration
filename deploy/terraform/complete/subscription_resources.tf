# Subscription Resources Management
# This file handles resource group creation and permission assignments across monitored subscriptions
# Mirrors the logic from subscription_permissions.bicep and resource_group_permissions.bicep

# Function to generate subscription UUID (mimics ARM template subUuid function)
# This is used for consistent naming across deployments
locals {
  sub_uuids = {
    for sub_id in local.monitored_subscriptions_final : sub_id => 
      substr(lower(uuidv5("dns", "${sub_id}-${local.control_plane_id}")), 24, 12)
  }
}

# Create resource groups in each monitored subscription (mimics subscription_permissions.bicep:12-15)
# Note: Resource group creation across multiple subscriptions requires separate Terraform runs per subscription
# or using a management script. For now, we'll document this requirement.
# resource "azurerm_resource_group" "forwarder_resource_groups" {
#   for_each = var.create_resource_groups_in_subscriptions ? toset(local.monitored_subscriptions_final) : []
# 
#   name     = var.resource_group_name
#   location = var.location
# 
#   # Note: This requires running Terraform separately for each subscription
#   # or using azurerm provider aliases configured for each subscription
# }

# Data source for built-in Azure roles (matching ARM template role definitions)
data "azurerm_role_definition" "monitoring_reader_builtin" {
  name = "Monitoring Reader"
}

data "azurerm_role_definition" "monitoring_contributor_builtin" {
  name = "Monitoring Contributor" 
}

data "azurerm_role_definition" "contributor_builtin" {
  name = "Contributor"
}

data "azurerm_role_definition" "reader_and_data_access_builtin" {
  name = "Reader and Data Access"
}

# Subscription-level permissions (mimics subscription_permissions.bicep role assignments)

# Resources Task - Monitoring Reader at subscription level (subscription_permissions.bicep:40-47)
resource "azurerm_role_assignment" "resources_task_subscription_monitoring_reader" {
  for_each = toset(local.monitored_subscriptions_final)

  scope              = "/subscriptions/${each.value}"
  role_definition_id = data.azurerm_role_definition.monitoring_reader_builtin.id
  principal_id       = azurerm_linux_function_app.resources_task.identity[0].principal_id
  description        = "ddlfo${local.control_plane_id}"

  # Use consistent naming like ARM template
  # ARM: guid(subscription().id, 'resourceTask', controlPlaneId)
  # Note: Terraform doesn't support custom GUIDs, so we'll let Azure generate them
}

# Diagnostic Settings Task - Monitoring Contributor at subscription level (subscription_permissions.bicep:49-56)
resource "azurerm_role_assignment" "diagnostic_settings_task_subscription_monitoring_contributor" {
  for_each = toset(local.monitored_subscriptions_final)

  scope              = "/subscriptions/${each.value}"
  role_definition_id = data.azurerm_role_definition.monitoring_contributor_builtin.id
  principal_id       = azurerm_linux_function_app.diagnostic_settings_task.identity[0].principal_id
  description        = "ddlfo${local.control_plane_id}"
}

# Initial Run (Deployer Task) - Monitoring Contributor at subscription level (subscription_permissions.bicep:59-66)
resource "azurerm_role_assignment" "deployer_task_subscription_monitoring_contributor" {
  for_each = toset(local.monitored_subscriptions_final)

  scope              = "/subscriptions/${each.value}"
  role_definition_id = data.azurerm_role_definition.monitoring_contributor_builtin.id
  principal_id       = azurerm_container_app_job.deployer_task.identity[0].principal_id
  description        = "ddlfo${local.control_plane_id}"
}

# Resource Group level permissions (mimics resource_group_permissions.bicep)
# Note: These assume resource groups exist in the target subscriptions

# Diagnostic Settings Task - Reader and Data Access at resource group level (resource_group_permissions.bicep:20-28)
resource "azurerm_role_assignment" "diagnostic_settings_task_rg_storage_access" {
  for_each = toset(local.monitored_subscriptions_final)

  scope              = "/subscriptions/${each.value}/resourceGroups/${var.resource_group_name}"
  role_definition_id = data.azurerm_role_definition.reader_and_data_access_builtin.id
  principal_id       = azurerm_linux_function_app.diagnostic_settings_task.identity[0].principal_id
  description        = "ddlfo${local.control_plane_id}"
}

# Scaling Task - Contributor at resource group level (resource_group_permissions.bicep:30-38) 
resource "azurerm_role_assignment" "scaling_task_rg_contributor" {
  for_each = toset(local.monitored_subscriptions_final)

  scope              = "/subscriptions/${each.value}/resourceGroups/${var.resource_group_name}"
  role_definition_id = data.azurerm_role_definition.contributor_builtin.id
  principal_id       = azurerm_linux_function_app.scaling_task.identity[0].principal_id
  description        = "ddlfo${local.control_plane_id}"
}

# Initial Run (Deployer Task) - Contributor at resource group level (resource_group_permissions.bicep:40-48)
resource "azurerm_role_assignment" "deployer_task_rg_contributor" {
  for_each = toset(local.monitored_subscriptions_final)

  scope              = "/subscriptions/${each.value}/resourceGroups/${var.resource_group_name}"
  role_definition_id = data.azurerm_role_definition.contributor_builtin.id
  principal_id       = azurerm_container_app_job.deployer_task.identity[0].principal_id
  description        = "ddlfo${local.control_plane_id}"
}