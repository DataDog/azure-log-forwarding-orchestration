#!/bin/bash

# Test script to verify APM deployment configuration
# This script checks that APM settings are properly configured in deployed resources

set -e

echo "========================================"
echo "APM Deployment Configuration Test"
echo "========================================"

# Check environment variables
echo ""
echo "Checking environment variables..."
echo "--------------------------------"

# Set default values if not provided
export LFO_VM_BASE_NAME=${LFO_VM_BASE_NAME:-"lfoms$(echo $USER | tr -d ' -' | tr '[:upper:]' '[:lower:]')"}
export DD_API_KEY=${DD_API_KEY:-""}
export DD_SITE=${DD_SITE:-"datadoghq.com"}
export DD_APM_ENABLED=${DD_APM_ENABLED:-"true"}
export DD_ENV=${DD_ENV:-"personal"}
export DD_SERVICE=${DD_SERVICE:-"azure-log-forwarder"}
export DD_VERSION=${DD_VERSION:-"latest"}

echo "LFO_VM_BASE_NAME: $LFO_VM_BASE_NAME"
echo "DD_SITE: $DD_SITE"
echo "DD_APM_ENABLED: $DD_APM_ENABLED"
echo "DD_ENV: $DD_ENV"
echo "DD_SERVICE: $DD_SERVICE"
echo "DD_VERSION: $DD_VERSION"

if [ -z "$DD_API_KEY" ]; then
    echo "❌ DD_API_KEY is not set. Please set it before running deployment scripts."
    exit 1
else
    echo "✅ DD_API_KEY is set (hidden for security)"
fi

# Check if resource group exists (for container app deployment)
echo ""
echo "Checking Container App Deployment..."
echo "------------------------------------"

RG_NAME=$(echo "$LFO_VM_BASE_NAME" | sed 's/[^a-zA-Z0-9]//g' | cut -c1-90)
echo "Resource Group Name: $RG_NAME"

if az group exists --name "$RG_NAME" 2>/dev/null | grep -q "true"; then
    echo "✅ Resource group exists"

    # Check container apps
    echo ""
    echo "Checking Container App Jobs..."
    JOBS=$(az containerapp job list --resource-group "$RG_NAME" --query "[].{name:name, envVars:properties.template.containers[0].env[?name=='DD_APM_ENABLED' || name=='DD_ENV' || name=='DD_SERVICE' || name=='DD_VERSION'].{name:name, value:value}}" -o json 2>/dev/null || echo "[]")

    if [ "$JOBS" != "[]" ]; then
        echo "$JOBS" | jq -r '.[] | "Job: \(.name)"'
        echo "$JOBS" | jq -r '.[] | .envVars[] | "  \(.name): \(.value)"'
    else
        echo "⚠️  No container app jobs found or unable to retrieve"
    fi

    # Check function apps
    echo ""
    echo "Checking Function Apps..."
    FUNCTION_APPS=$(az functionapp list --resource-group "$RG_NAME" --query "[].{name:name}" -o json 2>/dev/null || echo "[]")

    if [ "$FUNCTION_APPS" != "[]" ]; then
        for APP_NAME in $(echo "$FUNCTION_APPS" | jq -r '.[].name'); do
            echo "Function App: $APP_NAME"
            APP_SETTINGS=$(az functionapp config appsettings list --name "$APP_NAME" --resource-group "$RG_NAME" --query "[?name=='DD_APM_ENABLED' || name=='DD_ENV' || name=='DD_SERVICE' || name=='DD_VERSION'].{name:name, value:value}" -o json 2>/dev/null || echo "[]")
            if [ "$APP_SETTINGS" != "[]" ]; then
                echo "$APP_SETTINGS" | jq -r '.[] | "  \(.name): \(.value)"'
            else
                echo "  No APM settings found"
            fi
        done
    else
        echo "⚠️  No function apps found"
    fi
else
    echo "⚠️  Resource group does not exist. Run deploy_personal_env.py first."
fi

# Check VM deployment (if exists)
echo ""
echo "Checking VM Deployment..."
echo "-------------------------"

VM_RG_NAME="${LFO_VM_BASE_NAME}-forwarder-rg"
echo "VM Resource Group Name: $VM_RG_NAME"

if az group exists --name "$VM_RG_NAME" 2>/dev/null | grep -q "true"; then
    echo "✅ VM resource group exists"

    # Check VM extensions for custom script that might contain APM settings
    VM_NAME="${LFO_VM_BASE_NAME}-forwarder-vm"
    if az vm show --name "$VM_NAME" --resource-group "$VM_RG_NAME" &>/dev/null; then
        echo "VM: $VM_NAME"

        # Check if the custom script extension contains APM settings
        EXTENSION_SETTINGS=$(az vm extension show --vm-name "$VM_NAME" --resource-group "$VM_RG_NAME" --name "customScript" --query "settings.commandToExecute" -o tsv 2>/dev/null || echo "")

        if [[ "$EXTENSION_SETTINGS" == *"DD_APM_ENABLED"* ]]; then
            echo "  ✅ DD_APM_ENABLED found in VM setup script"
        else
            echo "  ⚠️  DD_APM_ENABLED not found in VM setup script"
        fi

        if [[ "$EXTENSION_SETTINGS" == *"DD_ENV"* ]]; then
            echo "  ✅ DD_ENV found in VM setup script"
        else
            echo "  ⚠️  DD_ENV not found in VM setup script"
        fi
    else
        echo "⚠️  VM not found"
    fi
else
    echo "⚠️  VM resource group does not exist. Run deploy_personal_forwarder_vm.py if needed."
fi

# Test building the forwarder with APM
echo ""
echo "Testing Forwarder Build with APM..."
echo "-----------------------------------"

cd forwarder 2>/dev/null || { echo "❌ forwarder directory not found"; exit 1; }

# Check if dd-trace-go is in go.mod
if grep -q "dd-trace-go" go.mod; then
    echo "✅ dd-trace-go dependency found in go.mod"
    DD_TRACE_VERSION=$(grep "dd-trace-go" go.mod | awk '{print $2}')
    echo "  Version: $DD_TRACE_VERSION"
else
    echo "❌ dd-trace-go dependency not found in go.mod"
fi

# Check if APM code exists in forwarder
if grep -q "tracer.Start" cmd/forwarder/forwarder.go; then
    echo "✅ APM tracer initialization found in forwarder.go"
else
    echo "❌ APM tracer initialization not found in forwarder.go"
fi

# Try to build the forwarder
echo ""
echo "Attempting to build forwarder..."
if go build -o /tmp/test-forwarder cmd/forwarder/forwarder.go 2>/dev/null; then
    echo "✅ Forwarder builds successfully with APM"
    rm -f /tmp/test-forwarder
else
    echo "❌ Forwarder build failed"
fi

echo ""
echo "========================================"
echo "Test Summary"
echo "========================================"
echo ""
echo "APM Configuration Checklist:"
echo "  [$([ "$DD_APM_ENABLED" == "true" ] && echo "✓" || echo " ")] DD_APM_ENABLED is set to 'true'"
echo "  [$([ -n "$DD_API_KEY" ] && echo "✓" || echo " ")] DD_API_KEY is configured"
echo "  [$([ -n "$DD_ENV" ] && echo "✓" || echo " ")] DD_ENV is configured"
echo "  [$([ -n "$DD_SERVICE" ] && echo "✓" || echo " ")] DD_SERVICE is configured"
echo "  [$(grep -q "dd-trace-go" go.mod 2>/dev/null && echo "✓" || echo " ")] dd-trace-go dependency added"
echo "  [$(grep -q "tracer.Start" cmd/forwarder/forwarder.go 2>/dev/null && echo "✓" || echo " ")] APM tracer initialized in code"
echo ""
echo "Next Steps:"
echo "1. Run deployment script: python scripts/deploy_personal_env.py"
echo "2. Or for VM: python scripts/deploy_personal_forwarder_vm.py"
echo "3. Check Datadog APM dashboard for traces from '$DD_SERVICE' service"
echo "4. Look for spans in environment '$DD_ENV'"
echo ""
