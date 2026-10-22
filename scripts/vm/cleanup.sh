#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

usage() {
    echo "Usage: $0 [--force]"
    echo "       /cleanup [--force]"
    echo ""
    echo "Delete your entire personal environment (LFO or forwarder) (destructive!)."
    echo ""
    echo "Options:"
    echo "  --force    Skip confirmation prompts (use with caution!)"
    echo "  --help     Show this help message"
    echo ""
    echo "WARNING: This is a destructive operation that cannot be undone!"
}

# Parse arguments
FORCE_DELETE=false
for arg in "$@"; do
    case $arg in
        --force)
            FORCE_DELETE=true
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            ;;
    esac
done

# Source both discovery libraries
source "${REPO_ROOT}/scripts/lfo/lib/lfo-discovery.sh"
source "${REPO_ROOT}/scripts/vm/lib/azure-discovery.sh"

echo "🧹 Cleanup Personal Environment"
echo "================================"
echo ""

# Discover resources - try LFO first, then VM forwarder
echo "🔍 Discovering resources to delete..."
LFO_ENV_TYPE=""
if discover_lfo_resources 2>/dev/null; then
    : # LFO_ENV_TYPE set to "lfo"
elif discover_resources 2>/dev/null; then
    : # LFO_ENV_TYPE set to "forwarder"
fi

if [ -z "$LFO_ENV_TYPE" ]; then
    echo "❌ No resources found to delete."
    echo ""
    echo "Available resource groups containing 'lfo' or your username:"
    CLEAN_USERNAME="${USER//./}"
    az group list --query "[?contains(name, 'lfo') || contains(name, '${CLEAN_USERNAME}')].name" -o tsv 2>/dev/null || echo "   (none found)"
    exit 1
fi

# Validate that we have a resource group to delete
if [ -z "$LFO_RESOURCE_GROUP" ]; then
    echo "❌ Resource group not found. Cannot proceed with deletion."
    echo ""
    echo "Try setting LFO_BASE_NAME or LFO_VM_BASE_NAME manually:"
    echo "  export LFO_BASE_NAME='your-base-name'       # for LFO environments"
    echo "  export LFO_VM_BASE_NAME='your-base-name'    # for forwarder environments"
    echo "  Then run the cleanup command again"
    exit 1
fi

# Display what will be deleted
echo ""
echo "⚠️  WARNING: This will permanently delete the following resources:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📁 Resource Group: $LFO_RESOURCE_GROUP"
echo ""

# List all resources in the group
echo "Resources to be deleted:"
az resource list --resource-group "$LFO_RESOURCE_GROUP" \
    --query "[].{Name:name, Type:type}" \
    --output table 2>/dev/null || echo "   Unable to list resources"

echo ""
echo "Environment Type: $LFO_ENV_TYPE"
echo ""
echo "This includes:"
if [ "$LFO_ENV_TYPE" = "lfo" ]; then
    if [ -n "${LFO_FUNCTION_APPS:-}" ]; then
        while IFS= read -r app; do
            [ -z "$app" ] && continue
            echo "   ✓ Function App: $app"
        done <<< "$LFO_FUNCTION_APPS"
    fi
    if [ -n "${LFO_CONTAINER_REGISTRY:-}" ]; then
        echo "   ✓ Container Registry: $LFO_CONTAINER_REGISTRY"
    fi
else
    if [ -n "${LFO_VM_NAME:-}" ]; then
        echo "   ✓ Virtual Machine: $LFO_VM_NAME (IP: ${LFO_VM_IP:-unknown})"
    fi
    if [ -n "${LFO_FUNCTION_APP:-}" ]; then
        echo "   ✓ Function App: $LFO_FUNCTION_APP"
    fi
fi
if [ -n "${LFO_STORAGE_ACCOUNT:-}" ]; then
    echo "   ✓ Storage Account: $LFO_STORAGE_ACCOUNT"
fi
echo "   ✓ All associated networking resources"
echo "   ✓ All managed identities and role assignments"
echo ""

# Check if force mode or get confirmation
if [ "$FORCE_DELETE" != "true" ]; then
    # Get confirmation
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    read -p "⚠️  Are you SURE you want to delete all these resources? Type 'yes' to confirm: " CONFIRM

    if [ "$CONFIRM" != "yes" ]; then
        echo ""
        echo "❌ Deletion cancelled. No resources were deleted."
        exit 0
    fi

    # Second confirmation for safety
    echo ""
    read -p "⚠️  This action CANNOT be undone. Type the resource group name '$LFO_RESOURCE_GROUP' to proceed: " CONFIRM_RG

    if [ "$CONFIRM_RG" != "$LFO_RESOURCE_GROUP" ]; then
        echo ""
        echo "❌ Resource group name did not match. Deletion cancelled."
        exit 0
    fi
else
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "⚠️  Force mode enabled - skipping confirmations"
    echo ""
fi

# Perform deletion
echo ""
echo "🗑️  Deleting resource group '$LFO_RESOURCE_GROUP'..."
echo "   This may take several minutes..."
echo ""

# Start deletion in background and show progress
az group delete \
    --name "$LFO_RESOURCE_GROUP" \
    --yes \
    --no-wait

# Monitor deletion progress
echo "Monitoring deletion progress..."
COUNTER=0
MAX_WAIT=600  # 10 minutes max

while [ $COUNTER -lt $MAX_WAIT ]; do
    # Check if resource group still exists
    EXISTS=$(az group exists --name "$LFO_RESOURCE_GROUP" 2>/dev/null || echo "false")

    if [ "$EXISTS" = "false" ]; then
        echo ""
        echo "✅ Resource group '$LFO_RESOURCE_GROUP' has been successfully deleted!"
        echo ""
        echo "Cleanup complete. All resources have been removed."

        # Clear environment variables if they were set
        echo ""
        echo "💡 To clean up environment variables, remove these from ~/.profile:"
        echo "   unset LFO_ENV_TYPE"
        if [ "${LFO_ENV_TYPE:-}" = "lfo" ]; then
            echo "   unset LFO_BASE_NAME"
            echo "   unset LFO_FUNCTION_APP"
            echo "   unset LFO_FUNCTION_KEY"
            echo "   unset LFO_RESOURCE_GROUP"
            echo "   unset LFO_STORAGE_ACCOUNT"
            echo "   unset LFO_CONTAINER_REGISTRY"
        else
            echo "   unset LFO_VM_BASE_NAME"
            echo "   unset LFO_VM_IP"
            echo "   unset LFO_FUNCTION_APP"
            echo "   unset LFO_FUNCTION_KEY"
            echo "   unset LFO_RESOURCE_GROUP"
            echo "   unset LFO_STORAGE_ACCOUNT"
        fi
        exit 0
    fi

    # Show progress indicator
    if [ $((COUNTER % 10)) -eq 0 ]; then
        echo -n "."
    fi

    sleep 1
    COUNTER=$((COUNTER + 1))
done

echo ""
echo "⚠️  Deletion is taking longer than expected."
echo "   The deletion will continue in the background."
echo "   You can check the status with:"
echo "   az group exists --name '$LFO_RESOURCE_GROUP'"
