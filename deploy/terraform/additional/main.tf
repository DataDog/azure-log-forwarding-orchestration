# Azure Log Forwarding Orchestration - Prerequisites Module
# This module creates the necessary resource group and permissions for a single monitored subscription

terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

# Data sources
data "azurerm_client_config" "current" {}

# Create the resource group for forwarder deployments
resource "azurerm_resource_group" "forwarder_rg" {
  name     = var.resource_group_name
  location = var.location

  tags = merge(
    var.tags,
    {
      "ManagedBy"   = "terraform"
      "Component"   = "forwarder-prerequisites"
      "Purpose"     = "log-forwarding-orchestration"
      "CreatedDate" = formatdate("YYYY-MM-DD", timestamp())
    }
  )
}

# Output the resource group information
output "resource_group_name" {
  description = "Name of the created resource group"
  value       = azurerm_resource_group.forwarder_rg.name
}

output "resource_group_id" {
  description = "ID of the created resource group"
  value       = azurerm_resource_group.forwarder_rg.id
}

output "resource_group_location" {
  description = "Location of the created resource group"
  value       = azurerm_resource_group.forwarder_rg.location
}

output "subscription_id" {
  description = "Subscription ID where the resource group was created"
  value       = var.subscription_id
}