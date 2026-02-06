#!/bin/bash

# Azure Log Forwarding Orchestration - Deployment Script
# This script deploys the complete control plane including the Resources Task

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}[HEADER]${NC} $1"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to validate prerequisites
validate_prerequisites() {
    print_header "Validating prerequisites..."
    
    # Check for required tools
    if ! command_exists terraform; then
        print_error "Terraform is not installed. Please install Terraform 1.0 or later."
        exit 1
    fi
    
    if ! command_exists az; then
        print_error "Azure CLI is not installed. Please install Azure CLI."
        exit 1
    fi
    
    if ! command_exists jq; then
        print_error "jq is not installed. Please install jq for JSON processing."
        exit 1
    fi
    
    # Check Terraform version
    TERRAFORM_VERSION=$(terraform version -json | jq -r '.terraform_version')
    print_status "Terraform version: $TERRAFORM_VERSION"
    
    # Check Azure CLI login
    if ! az account show >/dev/null 2>&1; then
        print_error "You are not logged in to Azure. Please run 'az login' first."
        exit 1
    fi
    
    # Display current Azure context
    CURRENT_SUBSCRIPTION=$(az account show --query 'name' -o tsv)
    CURRENT_TENANT=$(az account show --query 'tenantId' -o tsv)
    print_status "Current Azure subscription: $CURRENT_SUBSCRIPTION"
    print_status "Current Azure tenant: $CURRENT_TENANT"
    
    print_status "Prerequisites validation completed successfully!"
}

# Function to check if terraform.tfvars exists
check_tfvars() {
    print_header "Checking configuration..."
    
    if [ ! -f terraform.tfvars ]; then
        print_warning "terraform.tfvars file not found."
        print_status "Creating terraform.tfvars from example..."
        
        if [ -f terraform.tfvars.example ]; then
            cp terraform.tfvars.example terraform.tfvars
            print_status "terraform.tfvars created from example file."
            print_warning "Please edit terraform.tfvars with your specific values before proceeding."
            print_status "Required variables to update:"
            echo "  - resource_group_name"
            echo "  - datadog_api_key"
            echo "  - monitored_subscriptions"
            echo "  - location (optional)"
            echo ""
            read -p "Press Enter to continue after updating terraform.tfvars..."
        else
            print_error "terraform.tfvars.example not found. Cannot create terraform.tfvars."
            exit 1
        fi
    else
        print_status "terraform.tfvars found."
    fi
}

# Function to validate terraform.tfvars
validate_tfvars() {
    print_header "Validating terraform.tfvars..."
    
    # Check if required variables are set
    if ! grep -q "resource_group_name" terraform.tfvars || grep -q "my-datadog-log-forwarding-rg" terraform.tfvars; then
        print_error "Please update resource_group_name in terraform.tfvars"
        exit 1
    fi
    
    if ! grep -q "datadog_api_key" terraform.tfvars || grep -q "your-32-character-datadog-api-key" terraform.tfvars; then
        print_error "Please update datadog_api_key in terraform.tfvars"
        exit 1
    fi
    
    if ! grep -q "monitored_subscriptions" terraform.tfvars || grep -q "12345678-1234-1234-1234-123456789012" terraform.tfvars; then
        print_error "Please update monitored_subscriptions in terraform.tfvars"
        exit 1
    fi
    
    print_status "terraform.tfvars validation completed successfully!"
}

# Function to initialize Terraform
init_terraform() {
    print_header "Initializing Terraform..."
    
    terraform init
    
    if [ $? -eq 0 ]; then
        print_status "Terraform initialization completed successfully!"
    else
        print_error "Terraform initialization failed!"
        exit 1
    fi
}

# Function to format Terraform files
format_terraform() {
    print_header "Formatting Terraform files..."
    
    terraform fmt
    
    if [ $? -eq 0 ]; then
        print_status "Terraform formatting completed successfully!"
    else
        print_error "Terraform formatting failed!"
        exit 1
    fi
}

# Function to validate Terraform configuration
validate_terraform() {
    print_header "Validating Terraform configuration..."
    
    terraform validate
    
    if [ $? -eq 0 ]; then
        print_status "Terraform validation completed successfully!"
    else
        print_error "Terraform validation failed!"
        exit 1
    fi
}

# Function to plan Terraform deployment
plan_terraform() {
    print_header "Creating Terraform plan..."
    
    terraform plan -out=tfplan
    
    if [ $? -eq 0 ]; then
        print_status "Terraform plan created successfully!"
        print_status "Plan saved to tfplan file."
        return 0
    else
        print_error "Terraform plan failed!"
        exit 1
    fi
}

# Function to apply Terraform deployment
apply_terraform() {
    print_header "Applying Terraform deployment..."
    
    print_warning "This will create Azure resources. Are you sure you want to continue?"
    read -p "Type 'yes' to continue: " confirm
    
    if [ "$confirm" != "yes" ]; then
        print_status "Deployment cancelled."
        exit 0
    fi
    
    terraform apply tfplan
    
    if [ $? -eq 0 ]; then
        print_status "Terraform deployment completed successfully!"
        return 0
    else
        print_error "Terraform deployment failed!"
        exit 1
    fi
}

# Function to show deployment outputs
show_outputs() {
    print_header "Deployment outputs:"
    
    terraform output
    
    # Save outputs to a file
    terraform output -json > outputs.json
    print_status "Outputs saved to outputs.json"
    
    # Extract key information
    FUNCTION_APP_NAME=$(terraform output -raw resources_task_function_app_name 2>/dev/null || echo "N/A")
    STORAGE_ACCOUNT_NAME=$(terraform output -raw storage_account_name 2>/dev/null || echo "N/A")
    CONTROL_PLANE_ID=$(terraform output -raw control_plane_id 2>/dev/null || echo "N/A")
    
    print_status "Key deployment information:"
    echo "  Function App Name: $FUNCTION_APP_NAME"
    echo "  Storage Account Name: $STORAGE_ACCOUNT_NAME"
    echo "  Control Plane ID: $CONTROL_PLANE_ID"
}

# Function to run post-deployment checks
post_deployment_checks() {
    print_header "Running post-deployment checks..."
    
    # Check function app status
    FUNCTION_APP_NAME=$(terraform output -raw resources_task_function_app_name 2>/dev/null)
    RESOURCE_GROUP_NAME=$(terraform output -raw resource_group_name 2>/dev/null)
    
    if [ "$FUNCTION_APP_NAME" != "N/A" ] && [ "$RESOURCE_GROUP_NAME" != "N/A" ]; then
        print_status "Checking function app status..."
        
        APP_STATUS=$(az functionapp show --name "$FUNCTION_APP_NAME" --resource-group "$RESOURCE_GROUP_NAME" --query "state" -o tsv 2>/dev/null || echo "Unknown")
        print_status "Function app status: $APP_STATUS"
        
        if [ "$APP_STATUS" = "Running" ]; then
            print_status "Function app is running successfully!"
        else
            print_warning "Function app is not running. Status: $APP_STATUS"
        fi
    fi
    
    print_status "Post-deployment checks completed."
}

# Function to clean up
cleanup() {
    print_header "Cleaning up..."
    
    if [ -f tfplan ]; then
        rm tfplan
        print_status "Removed tfplan file."
    fi
}

# Function to show help
show_help() {
    cat << EOF
Azure Log Forwarding Orchestration - Deployment Script

Usage: $0 [OPTIONS]

Options:
  -h, --help          Show this help message
  -p, --plan-only     Only run plan, don't apply
  -a, --apply-only    Only apply (assumes plan exists)
  -d, --destroy       Destroy the deployment
  -v, --validate      Only validate configuration
  -f, --format        Only format Terraform files
  -c, --check         Only run checks without deployment

Examples:
  $0                  # Run full deployment
  $0 --plan-only      # Only create deployment plan
  $0 --apply-only     # Only apply existing plan
  $0 --destroy        # Destroy deployment
  $0 --validate       # Only validate configuration

Environment Variables:
  SKIP_VALIDATION     # Skip terraform validation if set to 'true'
  AUTO_APPROVE        # Auto approve deployment if set to 'true'

EOF
}

# Function to destroy deployment
destroy_deployment() {
    print_header "Destroying Terraform deployment..."
    
    print_warning "This will DESTROY all Azure resources created by this deployment!"
    print_warning "This action cannot be undone!"
    read -p "Type 'yes' to continue with destruction: " confirm
    
    if [ "$confirm" != "yes" ]; then
        print_status "Destruction cancelled."
        exit 0
    fi
    
    terraform destroy
    
    if [ $? -eq 0 ]; then
        print_status "Terraform destruction completed successfully!"
    else
        print_error "Terraform destruction failed!"
        exit 1
    fi
}

# Main deployment function
main() {
    local plan_only=false
    local apply_only=false
    local destroy=false
    local validate_only=false
    local format_only=false
    local check_only=false
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -p|--plan-only)
                plan_only=true
                shift
                ;;
            -a|--apply-only)
                apply_only=true
                shift
                ;;
            -d|--destroy)
                destroy=true
                shift
                ;;
            -v|--validate)
                validate_only=true
                shift
                ;;
            -f|--format)
                format_only=true
                shift
                ;;
            -c|--check)
                check_only=true
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # Print script header
    print_header "Azure Log Forwarding Orchestration - Deployment Script"
    print_status "Starting deployment process..."
    
    # Set up cleanup trap
    trap cleanup EXIT
    
    # Validate prerequisites
    validate_prerequisites
    
    # Handle format only
    if [ "$format_only" = true ]; then
        format_terraform
        exit 0
    fi
    
    # Handle check only
    if [ "$check_only" = true ]; then
        check_tfvars
        validate_tfvars
        exit 0
    fi
    
    # Handle destroy
    if [ "$destroy" = true ]; then
        destroy_deployment
        exit 0
    fi
    
    # Handle validate only
    if [ "$validate_only" = true ]; then
        init_terraform
        validate_terraform
        exit 0
    fi
    
    # Check and validate configuration
    check_tfvars
    validate_tfvars
    
    # Initialize Terraform
    init_terraform
    
    # Format Terraform files
    format_terraform
    
    # Validate Terraform configuration
    if [ "$SKIP_VALIDATION" != "true" ]; then
        validate_terraform
    fi
    
    # Handle apply only
    if [ "$apply_only" = true ]; then
        if [ ! -f tfplan ]; then
            print_error "No tfplan file found. Please run with --plan-only first."
            exit 1
        fi
        apply_terraform
        show_outputs
        post_deployment_checks
        exit 0
    fi
    
    # Create deployment plan
    plan_terraform
    
    # Handle plan only
    if [ "$plan_only" = true ]; then
        print_status "Plan created successfully. To apply, run: $0 --apply-only"
        exit 0
    fi
    
    # Apply deployment
    if [ "$AUTO_APPROVE" = "true" ]; then
        print_status "Auto-approve enabled, applying deployment..."
        terraform apply -auto-approve tfplan
    else
        apply_terraform
    fi
    
    # Show outputs
    show_outputs
    
    # Run post-deployment checks
    post_deployment_checks
    
    print_status "Deployment completed successfully!"
    print_status "Check the README.md for next steps and monitoring information."
}

# Run main function with all arguments
main "$@" 