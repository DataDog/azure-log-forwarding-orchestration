#!/bin/bash

# Azure Log Forwarding Orchestration - Terraform Validation Script
# This script validates the Terraform configuration for the complete control plane

set -e

echo "🔍 Validating Terraform configuration..."

# Check if required tools are installed
command -v terraform >/dev/null 2>&1 || { echo "❌ terraform is required but not installed. Aborting." >&2; exit 1; }
command -v az >/dev/null 2>&1 || { echo "❌ Azure CLI is required but not installed. Aborting." >&2; exit 1; }

# Check if user is logged in to Azure
if ! az account show >/dev/null 2>&1; then
    echo "❌ You are not logged in to Azure. Please run 'az login' first."
    exit 1
fi

echo "✅ Prerequisites check passed"

# Initialize Terraform
echo "🏗️  Initializing Terraform..."
terraform init

# Validate Terraform configuration
echo "🔍 Validating Terraform configuration..."
terraform validate

# Check if terraform.tfvars exists
if [ ! -f terraform.tfvars ]; then
    echo "⚠️  terraform.tfvars file not found. Please create one based on terraform.tfvars.example"
    echo "   You can copy the example file: cp terraform.tfvars.example terraform.tfvars"
    exit 1
fi

# Validate Management Group configuration (if enabled)
echo "🔍 Validating Management Group configuration..."
if terraform console <<< "var.use_management_group_discovery" 2>/dev/null | grep -q "true"; then
    MG_NAME=$(terraform console <<< "var.management_group_name" 2>/dev/null | tr -d '"')
    if [ "$MG_NAME" != "null" ]; then
        echo "✅ Management Group discovery enabled for: $MG_NAME"
        # Check if management group exists and we have access
        if az account management-group show --name "$MG_NAME" >/dev/null 2>&1; then
            echo "✅ Management Group '$MG_NAME' found and accessible"
            SUB_COUNT=$(az account management-group show --name "$MG_NAME" --expand --query "children[?type=='Microsoft.Management/managementGroups'].children[?type=='Microsoft.Billing/billingAccounts'].children[].name | length(@)" 2>/dev/null || echo "0")
            echo "ℹ️  Found management group with subscription access"
        else
            echo "❌ Cannot access management group '$MG_NAME'. Please check:"
            echo "   1. Management group name is correct"
            echo "   2. You have 'Management Group Reader' role on the management group"
            echo "   3. Management group exists in your Azure AD tenant"
            exit 1
        fi
    else
        echo "❌ Management Group discovery enabled but no management group name provided"
        exit 1
    fi
else
    echo "ℹ️  Using manual subscription list (management group discovery disabled)"
fi

# Format check
echo "🎨 Checking Terraform formatting..."
if ! terraform fmt -check; then
    echo "⚠️  Terraform files are not properly formatted. Run 'terraform fmt' to fix."
    exit 1
fi

# Plan validation
echo "📋 Creating Terraform plan..."
terraform plan -out=tfplan

echo "✅ Terraform validation completed successfully!"
echo ""
echo "Next steps:"
echo "1. Review the plan above"
echo "2. Run 'terraform apply tfplan' to deploy the resources"
echo "3. Monitor the deployment in the Azure portal"
echo ""
echo "📚 For more information, see the README.md file" 