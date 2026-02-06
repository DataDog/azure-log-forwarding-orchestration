# Data sources
data "azurerm_client_config" "current" {}

data "azurerm_resource_group" "current" {
  name = var.resource_group_name
}

# Management Group data source for subscription discovery
data "azurerm_management_group" "target" {
  count = var.use_management_group_discovery ? 1 : 0
  name  = var.management_group_name
}

# Generate a unique control plane ID based on subscription, resource group, and location
# This mimics the Bicep logic for consistent deployments
resource "random_string" "control_plane_id" {
  length  = 12
  special = false
  upper   = false
}

# Locals are now defined in locals.tf

# App Service Plan (Consumption Plan)
resource "azurerm_service_plan" "control_plane_asp" {
  name                = local.resource_names.app_service_plan
  location            = var.location
  resource_group_name = data.azurerm_resource_group.current.name
  os_type             = "Linux"
  sku_name            = "Y1"

  tags = local.common_tags
}

# Storage Account for Control Plane
resource "azurerm_storage_account" "control_plane_storage" {
  name                       = local.resource_names.storage_account
  resource_group_name        = data.azurerm_resource_group.current.name
  location                   = var.location
  account_tier               = "Standard"
  account_replication_type   = var.storage_account_replication_type
  account_kind               = "StorageV2"
  min_tls_version            = "TLS1_2"
  https_traffic_only_enabled = true

  blob_properties {
    change_feed_enabled = false
    versioning_enabled  = false
  }

  tags = local.common_tags
}

# File Service for storage account
resource "azurerm_storage_share" "control_plane_fileshare" {
  name               = local.resource_names.file_share
  storage_account_id = azurerm_storage_account.control_plane_storage.id
  quota              = 50
}

# Blob Services
resource "azurerm_storage_container" "cache_container" {
  name                  = local.resource_names.cache_container
  storage_account_id    = azurerm_storage_account.control_plane_storage.id
  container_access_type = "private"
}

# Resources Task Function App
resource "azurerm_linux_function_app" "resources_task" {
  name                = local.resource_names.function_app
  location            = var.location
  resource_group_name = data.azurerm_resource_group.current.name
  service_plan_id     = azurerm_service_plan.control_plane_asp.id

  storage_account_name       = azurerm_storage_account.control_plane_storage.name
  storage_account_access_key = azurerm_storage_account.control_plane_storage.primary_access_key

  identity {
    type = "SystemAssigned"
  }

  site_config {
    application_stack {
      python_version = "3.11"
    }

    dynamic "cors" {
      for_each = length(var.allowed_origins) > 0 ? [1] : []
      content {
        allowed_origins = local.cors_config.allowed_origins
      }
    }
  }

  app_settings = local.common_app_settings

  https_only = local.security_config.https_only

  tags = local.common_tags

  depends_on = [
    azurerm_storage_share.control_plane_fileshare
  ]
}

# Scaling Task Function App
resource "azurerm_linux_function_app" "scaling_task" {
  name                = local.resource_names.scaling_task
  location            = var.location
  resource_group_name = data.azurerm_resource_group.current.name
  service_plan_id     = azurerm_service_plan.control_plane_asp.id

  storage_account_name       = azurerm_storage_account.control_plane_storage.name
  storage_account_access_key = azurerm_storage_account.control_plane_storage.primary_access_key

  identity {
    type = "SystemAssigned"
  }

  site_config {
    application_stack {
      python_version = "3.11"
    }

    dynamic "cors" {
      for_each = length(var.allowed_origins) > 0 ? [1] : []
      content {
        allowed_origins = local.cors_config.allowed_origins
      }
    }
  }

  app_settings = merge(local.common_app_settings, {
    # Scaling Task Specific Settings
    "RESOURCE_GROUP"       = var.resource_group_name
    "FORWARDER_IMAGE"      = var.forwarder_image
    "CONTROL_PLANE_REGION" = var.location
    "PII_SCRUBBER_RULES"   = var.pii_scrubber_rules
    "WEBSITE_CONTENTSHARE" = local.resource_names.file_share
  })

  https_only = local.security_config.https_only

  tags = local.common_tags

  depends_on = [
    azurerm_storage_share.control_plane_fileshare
  ]
}

# Diagnostic Settings Task Function App
resource "azurerm_linux_function_app" "diagnostic_settings_task" {
  name                = local.resource_names.diagnostic_settings_task
  location            = var.location
  resource_group_name = data.azurerm_resource_group.current.name
  service_plan_id     = azurerm_service_plan.control_plane_asp.id

  storage_account_name       = azurerm_storage_account.control_plane_storage.name
  storage_account_access_key = azurerm_storage_account.control_plane_storage.primary_access_key

  identity {
    type = "SystemAssigned"
  }

  site_config {
    application_stack {
      python_version = "3.11"
    }

    dynamic "cors" {
      for_each = length(var.allowed_origins) > 0 ? [1] : []
      content {
        allowed_origins = local.cors_config.allowed_origins
      }
    }
  }

  app_settings = merge(local.common_app_settings, {
    # Diagnostic Settings Task Specific Settings
    "RESOURCE_GROUP"       = var.resource_group_name
    "WEBSITE_CONTENTSHARE" = local.resource_names.file_share
  })

  https_only = local.security_config.https_only

  tags = local.common_tags

  depends_on = [
    azurerm_storage_share.control_plane_fileshare
  ]
}

# Function App Configuration - Timer Trigger
# Note: Function code and configuration will be deployed separately via CI/CD pipeline
# The function app infrastructure is ready to receive the deployment

# Container Apps Environment for Deployer Task
resource "azurerm_container_app_environment" "deployer_env" {
  name                = local.resource_names.container_app_env
  location            = var.location
  resource_group_name = data.azurerm_resource_group.current.name

  tags = local.common_tags
}

# Container App Job for Deployer Task
resource "azurerm_container_app_job" "deployer_task" {
  name                         = local.resource_names.deployer_task
  location                     = var.location
  resource_group_name          = data.azurerm_resource_group.current.name
  container_app_environment_id = azurerm_container_app_environment.deployer_env.id

  replica_timeout_in_seconds = local.container_app_config.replica_timeout_seconds
  replica_retry_limit        = local.container_app_config.replica_retry_limit

  # Schedule trigger configuration
  schedule_trigger_config {
    cron_expression          = local.container_app_config.schedule_cron_expression
    parallelism              = local.container_app_config.parallelism
    replica_completion_count = local.container_app_config.replica_completion_count
  }

  # Container template
  template {
    container {
      name   = local.resource_names.deployer_task
      image  = local.deployer_image_url
      cpu    = local.container_app_config.cpu_limit
      memory = local.container_app_config.memory_limit

      # Environment variables
      env {
        name        = "AzureWebJobsStorage"
        secret_name = local.container_app_config.connection_string_secret
      }
      env {
        name  = "SUBSCRIPTION_ID"
        value = data.azurerm_client_config.current.subscription_id
      }
      env {
        name  = "RESOURCE_GROUP"
        value = var.resource_group_name
      }
      env {
        name  = "CONTROL_PLANE_ID"
        value = local.control_plane_id
      }
      env {
        name  = "CONTROL_PLANE_REGION"
        value = var.location
      }
      env {
        name        = "DD_API_KEY"
        secret_name = local.container_app_config.dd_api_key_secret
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
        value = "https://ddazurelfo.blob.core.windows.net"
      }
      env {
        name  = "LOG_LEVEL"
        value = var.log_level
      }
    }
  }

  # Secrets
  secret {
    name  = local.container_app_config.connection_string_secret
    value = local.storage_connection_string
  }

  secret {
    name  = local.container_app_config.dd_api_key_secret
    value = var.datadog_api_key
  }

  # System assigned identity
  identity {
    type = "SystemAssigned"
  }

  tags = local.common_tags
}


# Data source for built-in Monitoring Reader role
data "azurerm_role_definition" "monitoring_reader" {
  name = "Monitoring Reader"
}

# Data source for built-in Monitoring Contributor role
data "azurerm_role_definition" "monitoring_contributor" {
  name = "Monitoring Contributor"
}

# Data source for built-in Contributor role
data "azurerm_role_definition" "contributor" {
  name = "Contributor"
}

# Data source for built-in Reader and Data Access role
data "azurerm_role_definition" "reader_and_data_access" {
  name = "Reader and Data Access"
}

# Role assignments are now managed in subscription_resources.tf
# This provides better organization and matches the ARM template structure

# Data source for built-in Website Contributor role
data "azurerm_role_definition" "website_contributor" {
  name = "Website Contributor"
}

# Role assignment for deployer task at resource group level (Website Contributor)
resource "azurerm_role_assignment" "deployer_task_website_contributor" {
  scope              = data.azurerm_resource_group.current.id
  role_definition_id = data.azurerm_role_definition.website_contributor.id
  principal_id       = azurerm_container_app_job.deployer_task.identity[0].principal_id
  description        = "ddlfo${local.control_plane_id}"
}




# Storage Account Lifecycle Management
resource "azurerm_storage_management_policy" "control_plane_lifecycle" {
  storage_account_id = azurerm_storage_account.control_plane_storage.id

  dynamic "rule" {
    for_each = local.storage_lifecycle_rules
    content {
      name    = rule.value.name
      enabled = rule.value.enabled

      filters {
        prefix_match = rule.value.filters.prefix_match
        blob_types   = rule.value.filters.blob_types
      }

      actions {
        base_blob {
          delete_after_days_since_modification_greater_than = rule.value.actions.base_blob.delete_after_days_since_modification_greater_than
        }
      }
    }
  }
} 