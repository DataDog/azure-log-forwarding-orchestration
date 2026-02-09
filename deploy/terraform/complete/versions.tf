terraform {
  required_version = ">= 1.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.4"
    }
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  subscription_id = var.subscription_id != null ? var.subscription_id : var.monitored_subscriptions[0]

  features {
    # Configure resource group deletion behavior
    resource_group {
      prevent_deletion_if_contains_resources = true
    }
  }
} 