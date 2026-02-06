# Required Variables
variable "resource_group_name" {
  description = "Name of the resource group where resources will be created"
  type        = string
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

variable "monitored_subscriptions" {
  description = "List of subscription IDs to monitor for resources (used when use_management_group_discovery is false)"
  type        = list(string)
  default     = []

  validation {
    condition     = var.use_management_group_discovery || length(var.monitored_subscriptions) > 0
    error_message = "Either enable management group discovery or specify monitored subscriptions."
  }
}

# Management Group Configuration
variable "use_management_group_discovery" {
  description = "Enable automatic subscription discovery from management group"
  type        = bool
  default     = false
}

variable "management_group_name" {
  description = "Name of the management group to discover subscriptions from (required when use_management_group_discovery is true)"
  type        = string
  default     = null

  validation {
    condition     = !var.use_management_group_discovery || var.management_group_name != null
    error_message = "Management group name must be specified when use_management_group_discovery is enabled."
  }
}

variable "excluded_subscriptions" {
  description = "List of subscription IDs to exclude from monitoring (only used with management group discovery)"
  type        = list(string)
  default     = []
}

variable "create_resource_groups_in_subscriptions" {
  description = "Create resource groups for forwarders in each monitored subscription"
  type        = bool
  default     = true
}

variable "subscription_id" {
  description = "The Azure subscription ID to use for the provider (defaults to first monitored subscription)"
  type        = string
  default     = null
}

# Optional Variables with Defaults
variable "location" {
  description = "Azure region where resources will be created"
  type        = string
  default     = "East US"
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
      "datad0g.com"
    ], var.datadog_site)
    error_message = "Datadog site must be one of the allowed values."
  }
}

variable "datadog_telemetry" {
  description = "Enable Datadog telemetry"
  type        = bool
  default     = false
}

variable "log_level" {
  description = "Log level for the application"
  type        = string
  default     = "INFO"

  validation {
    condition = contains([
      "DEBUG",
      "INFO",
      "WARNING",
      "ERROR",
      "CRITICAL"
    ], var.log_level)
    error_message = "Log level must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL."
  }
}

variable "resource_tag_filters" {
  description = "Comma-separated list of resource tag filters. Use '!' prefix to exclude tags."
  type        = string
  default     = ""
}

variable "control_plane_id" {
  description = "Unique identifier for the control plane. If not provided, a random ID will be generated."
  type        = string
  default     = null

  validation {
    condition     = var.control_plane_id == null || var.control_plane_id == "" || try(length(var.control_plane_id) <= 12 && can(regex("^[a-z0-9]+$", var.control_plane_id)), false)
    error_message = "Control plane ID must be lowercase alphanumeric and no more than 12 characters."
  }
}

variable "resources_task_schedule" {
  description = "Cron expression for the resources task schedule"
  type        = string
  default     = "0 */5 * * * *" # Every 5 minutes
}

variable "cache_retention_days" {
  description = "Number of days to retain cache blobs"
  type        = number
  default     = 7

  validation {
    condition     = var.cache_retention_days >= 1 && var.cache_retention_days <= 365
    error_message = "Cache retention days must be between 1 and 365."
  }
}

# Removed: application_insights_connection_string (not used)

# Removed: application_insights_instrumentation_key (not used)

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

# Advanced Configuration Variables
variable "function_app_always_on" {
  description = "Should the function app be always on (not applicable for consumption plan)"
  type        = bool
  default     = false
}

variable "function_app_use_32_bit_worker" {
  description = "Should the function app use 32-bit worker process"
  type        = bool
  default     = false
}

variable "storage_account_replication_type" {
  description = "Storage account replication type"
  type        = string
  default     = "LRS"

  validation {
    condition = contains([
      "LRS",
      "GRS",
      "ZRS",
      "GZRS"
    ], var.storage_account_replication_type)
    error_message = "Storage account replication type must be one of: LRS, GRS, ZRS, GZRS."
  }
}

variable "enable_backup" {
  description = "Enable backup for the function app"
  type        = bool
  default     = false
}

variable "backup_settings" {
  description = "Backup settings for the function app"
  type = object({
    name                     = string
    enabled                  = bool
    storage_account_url      = string
    frequency_interval       = number
    frequency_unit           = string
    retention_period_in_days = number
  })
  default = null
}

# Security Variables
variable "allowed_origins" {
  description = "List of allowed origins for CORS"
  type        = list(string)
  default     = []
}

variable "ip_restrictions" {
  description = "List of IP restrictions for the function app"
  type = list(object({
    ip_address                = optional(string)
    service_tag               = optional(string)
    virtual_network_subnet_id = optional(string)
    name                      = optional(string)
    priority                  = optional(number)
    action                    = optional(string)
    headers = optional(object({
      x_azure_fdid      = optional(list(string))
      x_fd_health_probe = optional(list(string))
      x_forwarded_for   = optional(list(string))
      x_forwarded_host  = optional(list(string))
    }))
  }))
  default = []
}

variable "scm_ip_restrictions" {
  description = "List of SCM IP restrictions for the function app"
  type = list(object({
    ip_address                = optional(string)
    service_tag               = optional(string)
    virtual_network_subnet_id = optional(string)
    name                      = optional(string)
    priority                  = optional(number)
    action                    = optional(string)
    headers = optional(object({
      x_azure_fdid      = optional(list(string))
      x_fd_health_probe = optional(list(string))
      x_forwarded_for   = optional(list(string))
      x_forwarded_host  = optional(list(string))
    }))
  }))
  default = []
}

# Monitoring and Alerting Variables
# Removed: enable_monitoring (not used)

# Removed: alert_email_addresses (not used)


# Scaling Task Variables
variable "forwarder_image" {
  description = "Container image for the forwarder"
  type        = string
  default     = "datadoghq.azurecr.io/forwarder:latest"

  validation {
    condition     = can(regex("^[a-zA-Z0-9][a-zA-Z0-9._-]*\\.[a-zA-Z0-9._-]+(/[a-zA-Z0-9][a-zA-Z0-9._-]*)*:[a-zA-Z0-9][a-zA-Z0-9._-]*$", var.forwarder_image))
    error_message = "Forwarder image must be a valid container image reference (registry/repository:tag)."
  }
}

variable "pii_scrubber_rules" {
  description = "YAML formatted list of PII Scrubber Rules"
  type        = string
  default     = ""

  validation {
    condition     = var.pii_scrubber_rules == "" || can(yamldecode(var.pii_scrubber_rules))
    error_message = "PII scrubber rules must be valid YAML format."
  }
}

# Deployer Task Variables
variable "image_registry" {
  description = "Container registry for the deployer and forwarder images"
  type        = string
  default     = "datadoghq.azurecr.io"

  validation {
    condition     = can(regex("^[a-zA-Z0-9][a-zA-Z0-9._-]*\\.[a-zA-Z0-9._-]+$", var.image_registry))
    error_message = "Image registry must be a valid container registry URL."
  }
}

variable "deployer_image_tag" {
  description = "Tag for the deployer container image"
  type        = string
  default     = "latest"
}

variable "storage_account_url" {
  description = "URL of the storage account for the deployer task"
  type        = string
  default     = null
} 