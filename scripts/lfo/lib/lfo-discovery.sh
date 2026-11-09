#!/bin/bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

# Common discovery functions for LFO (Log Forwarding Orchestration) environments
# Source this file: source "$(cd "$(dirname "$0")/../.." && pwd)/scripts/lfo/lib/lfo-discovery.sh"

# Function to discover LFO environment resources
# LFO environments have 3+ function apps and no VM (deployed via deploy_personal_env.py)
discover_lfo_resources() {
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

    # Build base name matching deploy_personal_env.py: lfo_base_name = sub(r"\W+", "", f"lfo{user}")
    local USERNAME="${USER:-unknown}"
    local CLEAN_USERNAME
    CLEAN_USERNAME=$(echo "$USERNAME" | tr -cd '[:alnum:]')
    local BASE_NAME="${LFO_BASE_NAME:-lfo${CLEAN_USERNAME}}"

    # Try resource group patterns (deploy_personal_env.py uses get_name() which returns base_name directly)
    local RESOURCE_GROUP=""
    local RG_EXISTS

    # Pattern 1: base name directly (matches deploy_personal_env.py)
    RG_EXISTS=$(az group exists --name "$BASE_NAME" 2>/dev/null || echo "false")
    if [ "$RG_EXISTS" = "true" ]; then
        RESOURCE_GROUP="$BASE_NAME"
    fi

    # Pattern 2: rg-BASE_NAME
    if [ -z "$RESOURCE_GROUP" ]; then
        RG_EXISTS=$(az group exists --name "rg-${BASE_NAME}" 2>/dev/null || echo "false")
        if [ "$RG_EXISTS" = "true" ]; then
            RESOURCE_GROUP="rg-${BASE_NAME}"
        fi
    fi

    # Pattern 3: BASE_NAMErg
    if [ -z "$RESOURCE_GROUP" ]; then
        RG_EXISTS=$(az group exists --name "${BASE_NAME}rg" 2>/dev/null || echo "false")
        if [ "$RG_EXISTS" = "true" ]; then
            RESOURCE_GROUP="${BASE_NAME}rg"
        fi
    fi

    # Fallback: search for any RG containing the clean username
    if [ -z "$RESOURCE_GROUP" ]; then
        echo "Searching for LFO resource groups..." >&2
        local FOUND_RG
        FOUND_RG=$(az group list --query "[?contains(name, '$CLEAN_USERNAME') && !contains(name, 'vm')].name | [0]" -o tsv 2>/dev/null)
        if [ -n "$FOUND_RG" ] && [ "$FOUND_RG" != "null" ]; then
            RESOURCE_GROUP="$FOUND_RG"
        fi
    fi

    if [ -z "$RESOURCE_GROUP" ]; then
        return 1
    fi

    # Validate this is an LFO environment: must have 3+ function apps
    local FUNC_APP_COUNT
    FUNC_APP_COUNT=$(az functionapp list --resource-group "$RESOURCE_GROUP" --query "length(@)" -o tsv 2>/dev/null || echo "0")
    if [ "$FUNC_APP_COUNT" -lt 3 ] 2>/dev/null; then
        return 1
    fi

    # This is an LFO environment - export discovered values
    export LFO_ENV_TYPE="lfo"
    export LFO_BASE_NAME="$BASE_NAME"
    export LFO_RESOURCE_GROUP="$RESOURCE_GROUP"

    # Get subscription ID for portal links
    export LFO_SUBSCRIPTION_ID
    LFO_SUBSCRIPTION_ID=$(az account show --query "id" -o tsv 2>/dev/null || echo "")

    # Get all function apps
    export LFO_FUNCTION_APPS
    LFO_FUNCTION_APPS=$(az functionapp list --resource-group "$RESOURCE_GROUP" --query "[].name" -o tsv 2>/dev/null || echo "")

    # Find Loggy function app (name containing "loggy")
    export LFO_FUNCTION_APP=""
    export LFO_FUNCTION_KEY=""
    local app
    while IFS= read -r app; do
        if [[ "$app" == *loggy* ]]; then
            LFO_FUNCTION_APP="$app"
            # Try to get function key for CustomLog
            LFO_FUNCTION_KEY=$(az functionapp function keys list \
                --resource-group "$RESOURCE_GROUP" \
                --name "$app" \
                --function-name "CustomLog" \
                --query "default" -o tsv 2>/dev/null || echo "")
            if [ "$LFO_FUNCTION_KEY" = "null" ]; then
                LFO_FUNCTION_KEY=""
            fi
            break
        fi
    done <<< "$LFO_FUNCTION_APPS"

    # Get Storage Account
    export LFO_STORAGE_ACCOUNT=""
    export LFO_STORAGE_CONNECTION_STRING=""
    LFO_STORAGE_ACCOUNT=$(az storage account list --resource-group "$RESOURCE_GROUP" --query "[0].name" -o tsv 2>/dev/null || echo "")
    if [ -n "$LFO_STORAGE_ACCOUNT" ] && [ "$LFO_STORAGE_ACCOUNT" != "null" ]; then
        LFO_STORAGE_CONNECTION_STRING=$(az storage account show-connection-string \
            --resource-group "$RESOURCE_GROUP" \
            --name "$LFO_STORAGE_ACCOUNT" \
            --query "connectionString" -o tsv 2>/dev/null || echo "")
        if [ "$LFO_STORAGE_CONNECTION_STRING" = "null" ]; then
            LFO_STORAGE_CONNECTION_STRING=""
        fi
    else
        LFO_STORAGE_ACCOUNT=""
    fi

    # Get Container Registry
    export LFO_CONTAINER_REGISTRY=""
    LFO_CONTAINER_REGISTRY=$(az acr list --resource-group "$RESOURCE_GROUP" --query "[0].name" -o tsv 2>/dev/null || echo "")
    if [ "$LFO_CONTAINER_REGISTRY" = "null" ]; then
        LFO_CONTAINER_REGISTRY=""
    fi

    return 0
}

# Auto-discover if run directly
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    discover_lfo_resources
    echo "LFO Environment:"
    echo "  Type: ${LFO_ENV_TYPE:-unknown}"
    echo "  Base Name: ${LFO_BASE_NAME:-Not found}"
    echo "  Resource Group: ${LFO_RESOURCE_GROUP:-Not found}"
    echo "  Function Apps: ${LFO_FUNCTION_APPS:-Not found}"
    echo "  Loggy App: ${LFO_FUNCTION_APP:-Not found}"
    echo "  Storage Account: ${LFO_STORAGE_ACCOUNT:-Not found}"
    echo "  Container Registry: ${LFO_CONTAINER_REGISTRY:-Not found}"
fi
