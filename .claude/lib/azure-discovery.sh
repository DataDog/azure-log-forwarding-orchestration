#!/bin/bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

# Common discovery functions for Azure resources
# Source this file in other skills: source $(dirname "$0")/common-discovery.sh

# Function to discover user's Azure resources
discover_resources() {
    # Check az CLI is installed
    if ! command -v az &>/dev/null; then
        echo "ERROR: Azure CLI (az) is not installed." >&2
        echo "  Install: https://learn.microsoft.com/en-us/cli/azure/install-azure-cli" >&2
        return 1
    fi

    # Check az CLI is logged in
    if ! az account show &>/dev/null; then
        echo "ERROR: Not logged in to Azure CLI." >&2
        echo "  Run: az login" >&2
        return 1
    fi

    # Get user's environment
    USERNAME="${USER:-unknown}"
    # Remove dots from username for Azure resource naming
    CLEAN_USERNAME="${USERNAME//./}"
    BASE_NAME="${LFO_VM_BASE_NAME:-lfo${CLEAN_USERNAME}vm}"

    # Support both naming conventions (old: lfousernamevm, new: lfo<date>)
    if [[ "$BASE_NAME" == lfo* ]]; then
        # Try different resource group patterns
        # Pattern 1: rg-BASE_NAME (new convention)
        RESOURCE_GROUP="rg-${BASE_NAME}"
        RG_EXISTS=$(az group exists --name "$RESOURCE_GROUP" 2>/dev/null || echo "false")

        if [ "$RG_EXISTS" != "true" ]; then
            # Pattern 2: BASE_NAMErg (older convention)
            RESOURCE_GROUP="${BASE_NAME}rg"
            RG_EXISTS=$(az group exists --name "$RESOURCE_GROUP" 2>/dev/null || echo "false")
        fi

        if [ "$RG_EXISTS" != "true" ]; then
            # Pattern 3: For dates like lfoms1829, try without 'vm' suffix
            BASE_WITHOUT_VM="${BASE_NAME%vm}"
            RESOURCE_GROUP="${BASE_WITHOUT_VM}rg"
            RG_EXISTS=$(az group exists --name "$RESOURCE_GROUP" 2>/dev/null || echo "false")
            if [ "$RG_EXISTS" = "true" ]; then
                BASE_NAME="${BASE_WITHOUT_VM}"
            fi
        fi

        if [ "$RG_EXISTS" != "true" ]; then
            # Try to find any resource group matching the pattern
            echo "Discovering resource groups..." >&2
            # Search for both the original username and cleaned username
            FOUND_RG=$(az group list --query "[?contains(name, '$CLEAN_USERNAME') || contains(name, '${BASE_NAME}')].name | [0]" -o tsv 2>/dev/null)
            if [ -n "$FOUND_RG" ]; then
                RESOURCE_GROUP="$FOUND_RG"
                # Extract base name from resource group
                BASE_NAME=$(echo "$RESOURCE_GROUP" | sed -e 's/^rg-//' -e 's/rg$//')
            fi
        fi
    fi

    # Export discovered values
    export LFO_VM_BASE_NAME="$BASE_NAME"
    export LFO_RESOURCE_GROUP="$RESOURCE_GROUP"

    # Get VM IP
    if [ -n "$RESOURCE_GROUP" ]; then
        VM_INFO=$(az vm list --resource-group "$RESOURCE_GROUP" --query "[0]" -o json 2>/dev/null || echo "{}")
        if [ "$VM_INFO" != "{}" ] && [ "$VM_INFO" != "null" ]; then
            VM_NAME=$(echo "$VM_INFO" | jq -r '.name // empty')
            if [ -n "$VM_NAME" ]; then
                VM_IP=$(az vm list-ip-addresses --resource-group "$RESOURCE_GROUP" --name "$VM_NAME" \
                        --query "[0].virtualMachine.network.publicIpAddresses[0].ipAddress" -o tsv 2>/dev/null)
                export LFO_VM_IP="${VM_IP}"
                export LFO_VM_NAME="${VM_NAME}"
            fi
        fi
    fi

    # Get Function App details
    if [ -n "$RESOURCE_GROUP" ]; then
        FUNCTION_APP=$(az functionapp list --resource-group "$RESOURCE_GROUP" --query "[0].name" -o tsv 2>/dev/null)
        if [ -n "$FUNCTION_APP" ] && [ "$FUNCTION_APP" != "null" ]; then
            export LFO_FUNCTION_APP="$FUNCTION_APP"

            # Try to get function key
            FUNCTION_KEY=$(az functionapp function keys list \
                          --resource-group "$RESOURCE_GROUP" \
                          --name "$FUNCTION_APP" \
                          --function-name "CustomLog" \
                          --query "default" -o tsv 2>/dev/null || echo "")
            if [ -n "$FUNCTION_KEY" ] && [ "$FUNCTION_KEY" != "null" ]; then
                export LFO_FUNCTION_KEY="$FUNCTION_KEY"
            fi
        fi
    fi

    # Get Storage Account
    if [ -n "$RESOURCE_GROUP" ]; then
        STORAGE_ACCOUNT=$(az storage account list --resource-group "$RESOURCE_GROUP" --query "[0].name" -o tsv 2>/dev/null)
        if [ -n "$STORAGE_ACCOUNT" ] && [ "$STORAGE_ACCOUNT" != "null" ]; then
            export LFO_STORAGE_ACCOUNT="$STORAGE_ACCOUNT"

            # Get connection string
            CONN_STR=$(az storage account show-connection-string \
                       --resource-group "$RESOURCE_GROUP" \
                       --name "$STORAGE_ACCOUNT" \
                       --query "connectionString" -o tsv 2>/dev/null || echo "")
            if [ -n "$CONN_STR" ] && [ "$CONN_STR" != "null" ]; then
                export LFO_STORAGE_CONNECTION_STRING="$CONN_STR"
            fi
        fi
    fi

    # Return 0 if we found at least the resource group
    if [ -n "$RESOURCE_GROUP" ]; then
        return 0
    else
        return 1
    fi
}

# Function to print discovered resources
print_discovered_resources() {
    echo "🔍 Discovered Resources:"
    echo "   Resource Group: ${LFO_RESOURCE_GROUP:-Not found}"
    echo "   VM Name: ${LFO_VM_NAME:-Not found}"
    echo "   VM IP: ${LFO_VM_IP:-Not found}"
    echo "   Function App: ${LFO_FUNCTION_APP:-Not found}"
    echo "   Storage Account: ${LFO_STORAGE_ACCOUNT:-Not found}"

    if [ -n "$LFO_FUNCTION_KEY" ]; then
        echo "   Function Key: ${LFO_FUNCTION_KEY:0:10}..."
    fi
}

# Auto-discover if sourced directly
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    discover_resources
    print_discovered_resources
fi
