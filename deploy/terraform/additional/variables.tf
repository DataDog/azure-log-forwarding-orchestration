# Variables for the prerequisites module

variable "subscription_id" {
  description = "The Azure subscription ID where the resource group will be created"
  type        = string

  validation {
    condition     = can(regex("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", var.subscription_id))
    error_message = "Subscription ID must be a valid UUID format."
  }
}

variable "resource_group_name" {
  description = "Name of the resource group to create for forwarder deployments"
  type        = string

  validation {
    condition     = can(regex("^[a-zA-Z0-9._-]{1,90}$", var.resource_group_name))
    error_message = "Resource group name must be 1-90 characters long and can contain letters, numbers, periods, underscores, and hyphens."
  }
}

variable "location" {
  description = "Azure region where the resource group will be created"
  type        = string

  validation {
    condition = contains([
      "eastus", "eastus2", "westus", "westus2", "westus3", "centralus", "northcentralus", "southcentralus", "westcentralus",
      "canadacentral", "canadaeast", "brazilsouth", "northeurope", "westeurope", "uksouth", "ukwest", "francecentral",
      "francesouth", "germanywestcentral", "norwayeast", "switzerlandnorth", "swedencentral", "eastasia", "southeastasia",
      "australiaeast", "australiasoutheast", "centralindia", "southindia", "westindia", "japaneast", "japanwest",
      "koreacentral", "koreasouth", "southafricanorth", "uaenorth", "qatarcentral"
    ], var.location)
    error_message = "Location must be a valid Azure region."
  }
}

variable "tags" {
  description = "Tags to apply to the resource group"
  type        = map(string)
  default     = {}

  validation {
    condition     = length(var.tags) <= 50
    error_message = "A maximum of 50 tags can be applied to each resource."
  }
}