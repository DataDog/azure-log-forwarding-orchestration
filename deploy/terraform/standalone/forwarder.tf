# Variables
variable "environment_name" {
  description = "Name of the Container App Managed Environment for the Forwarder"
  type        = string
  default     = "datadog-log-forwarder-env"

  validation {
    condition     = length(var.environment_name) >= 2 && length(var.environment_name) <= 60
    error_message = "Environment name must be between 2 and 60 characters long."
  }
}

variable "job_name" {
  description = "Name of the Forwarder Container App Job"
  type        = string
  default     = "datadog-log-forwarder"

  validation {
    condition     = length(var.job_name) >= 1 && length(var.job_name) <= 260
    error_message = "Job name must be between 1 and 260 characters long."
  }
}

variable "storage_account_name" {
  description = "Name of the Log Storage Account"
  type        = string
  default     = "datadoglogstorage"

  validation {
    condition     = length(var.storage_account_name) >= 3 && length(var.storage_account_name) <= 24
    error_message = "Storage account name must be between 3 and 24 characters long."
  }
}

variable "storage_account_sku" {
  description = "The SKU of the storage account"
  type        = string
  default     = "Standard_LRS"

  validation {
    condition = contains([
      "Premium_LRS",
      "Premium_ZRS",
      "Standard_GRS",
      "Standard_GZRS",
      "Standard_LRS",
      "Standard_ZRS"
    ], var.storage_account_sku)
    error_message = "Storage account SKU must be one of the allowed values."
  }
}

variable "storage_account_retention_days" {
  description = "The number of days to retain logs (and internal metrics) in the storage account"
  type        = number
  default     = 1

  validation {
    condition     = var.storage_account_retention_days >= 1
    error_message = "Storage account retention days must be at least 1."
  }
}

variable "datadog_api_key" {
  description = "Datadog API Key"
  type        = string
  sensitive   = true

  validation {
    condition     = length(var.datadog_api_key) == 32
    error_message = "Datadog API key must be exactly 32 characters long."
  }
}

variable "datadog_site" {
  description = "Datadog Site"
  type        = string
  default     = "datadoghq.com"

  validation {
    condition = contains([
      "datadoghq.com",
      "datadoghq.eu",
      "ap1.datadoghq.com",
      "ap2.datadoghq.com",
      "us3.datadoghq.com",
      "us5.datadoghq.com",
      "ddog-gov.com",
      "datad0g.com",
    ], var.datadog_site)
    error_message = "Datadog site must be one of the allowed values."
  }
}

variable "location" {
  description = "Azure region where resources will be created"
  type        = string
  default     = "East US"
}

variable "resource_group_name" {
  description = "Name of the resource group where resources will be created"
  type        = string
}

variable "forwarder_image" {
  description = "Container image for the forwarder"
  type        = string
  default     = "datadoghq.azurecr.io/forwarder:latest"
}

variable "forwarder_cpu" {
  description = "CPU allocation for the forwarder container"
  type        = number
  default     = 2
}

variable "forwarder_memory" {
  description = "Memory allocation for the forwarder container"
  type        = string
  default     = "4Gi"
}

variable "schedule_expression" {
  description = "Cron expression for the forwarder job schedule"
  type        = string
  default     = "* * * * *"
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

# Data source for current resource group
data "azurerm_resource_group" "current" {
  name = var.resource_group_name
}

# Storage Account
resource "azurerm_storage_account" "forwarder_storage" {
  name                     = var.storage_account_name
  resource_group_name      = data.azurerm_resource_group.current.name
  location                 = var.location
  account_tier             = split("_", var.storage_account_sku)[0]
  account_replication_type = split("_", var.storage_account_sku)[1]
  account_kind             = "StorageV2"
  min_tls_version          = "TLS1_2"
  https_traffic_only_enabled = true

  tags = var.tags
}

# Storage Account Management Policy
resource "azurerm_storage_management_policy" "forwarder_lifecycle" {
  storage_account_id = azurerm_storage_account.forwarder_storage.id

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

# Container App Environment
resource "azurerm_container_app_environment" "forwarder_env" {
  name                = var.environment_name
  location            = var.location
  resource_group_name = data.azurerm_resource_group.current.name

  tags = var.tags
}

# Local values for connection string
locals {
  storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=${azurerm_storage_account.forwarder_storage.name};EndpointSuffix=core.windows.net;AccountKey=${azurerm_storage_account.forwarder_storage.primary_access_key}"
}

# Data source for current client config
data "azurerm_client_config" "current" {}

# Container App Job
resource "azurerm_container_app_job" "forwarder" {
  name                         = var.job_name
  location                     = var.location
  resource_group_name          = data.azurerm_resource_group.current.name
  container_app_environment_id = azurerm_container_app_environment.forwarder_env.id

  replica_timeout_in_seconds = 1800
  replica_retry_limit        = 1

  schedule_trigger_config {
    cron_expression                = var.schedule_expression
    parallelism                    = 1
    replica_completion_count       = 1
  }

  template {
    container {
      name   = "datadog-forwarder"
      image  = var.forwarder_image
      cpu    = var.forwarder_cpu
      memory = var.forwarder_memory

      env {
        name        = "AzureWebJobsStorage"
        secret_name = "storage-connection-string"
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
        name  = "CONTROL_PLANE_ID"
        value = "none"
      }
      env {
        name  = "CONFIG_ID"
        value = "standalone-forwarder"
      }
    }
  }

  secret {
    name  = "storage-connection-string"
    value = local.storage_connection_string
  }

  secret {
    name  = "dd-api-key"
    value = var.datadog_api_key
  }

  tags = var.tags
}

# Outputs
output "storage_account_name" {
  description = "Name of the created storage account"
  value       = azurerm_storage_account.forwarder_storage.name
}

output "storage_account_id" {
  description = "ID of the created storage account"
  value       = azurerm_storage_account.forwarder_storage.id
}

output "storage_account_primary_access_key" {
  description = "Primary access key of the storage account"
  value       = azurerm_storage_account.forwarder_storage.primary_access_key
  sensitive   = true
}

output "container_app_environment_id" {
  description = "ID of the container app environment"
  value       = azurerm_container_app_environment.forwarder_env.id
}

output "container_app_job_id" {
  description = "ID of the container app job"
  value       = azurerm_container_app_job.forwarder.id
}

output "container_app_job_name" {
  description = "Name of the container app job"
  value       = azurerm_container_app_job.forwarder.name
}
