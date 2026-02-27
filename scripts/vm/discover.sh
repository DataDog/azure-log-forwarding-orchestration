#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

usage() {
    echo "Usage: $0 [--export]"
    echo "       /discover [--export]"
    echo ""
    echo "Discover and display your personal environment resources."
    echo "Supports both LFO (function app) and forwarder (VM) environments."
    echo ""
    echo "Options:"
    echo "  --export    Output environment variables for export"
    echo "  --help      Show this help message"
    echo ""
    echo "This command discovers your Azure resources based on your username."
    echo "It tries LFO environments first, then falls back to VM forwarder environments."
}

# Parse arguments
EXPORT_MODE=false
for arg in "$@"; do
    case $arg in
        --export)
            EXPORT_MODE=true
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

# Try LFO discovery first, then fall back to VM forwarder
LFO_ENV_TYPE=""
if discover_lfo_resources 2>/dev/null; then
    : # LFO_ENV_TYPE set to "lfo" by discover_lfo_resources
elif discover_resources 2>/dev/null; then
    : # LFO_ENV_TYPE set to "forwarder" by discover_resources
fi

# --- Export mode ---
if [ "$EXPORT_MODE" = "true" ]; then
    if [ "$LFO_ENV_TYPE" = "lfo" ]; then
        echo "export LFO_ENV_TYPE=\"lfo\""
        echo "export LFO_BASE_NAME=\"$LFO_BASE_NAME\""
        echo "export LFO_RESOURCE_GROUP=\"$LFO_RESOURCE_GROUP\""
        echo "export LFO_SUBSCRIPTION_ID=\"${LFO_SUBSCRIPTION_ID:-}\""
        echo "export LFO_FUNCTION_APPS=\"${LFO_FUNCTION_APPS:-}\""
        echo "export LFO_FUNCTION_APP=\"${LFO_FUNCTION_APP:-}\""
        echo "export LFO_FUNCTION_KEY=\"${LFO_FUNCTION_KEY:-}\""
        echo "export LFO_STORAGE_ACCOUNT=\"${LFO_STORAGE_ACCOUNT:-}\""
        echo "export LFO_CONTAINER_REGISTRY=\"${LFO_CONTAINER_REGISTRY:-}\""
    elif [ "$LFO_ENV_TYPE" = "forwarder" ]; then
        echo "export LFO_ENV_TYPE=\"forwarder\""
        echo "export LFO_VM_BASE_NAME=\"$LFO_VM_BASE_NAME\""
        echo "export LFO_VM_IP=\"${LFO_VM_IP:-}\""
        echo "export LFO_VM_NAME=\"${LFO_VM_NAME:-}\""
        echo "export LFO_FUNCTION_APP=\"${LFO_FUNCTION_APP:-}\""
        echo "export LFO_FUNCTION_KEY=\"${LFO_FUNCTION_KEY:-}\""
        echo "export LFO_RESOURCE_GROUP=\"$LFO_RESOURCE_GROUP\""
        echo "export LFO_SUBSCRIPTION_ID=\"${LFO_SUBSCRIPTION_ID:-}\""
        echo "export LFO_STORAGE_ACCOUNT=\"${LFO_STORAGE_ACCOUNT:-}\""
    fi
    exit 0
fi

# --- Display mode ---

if [ "$LFO_ENV_TYPE" = "lfo" ]; then
    # ========== LFO Environment ==========
    echo "🔍 Discovering LFO Environment"
    echo "==============================="
    echo "User: ${USER:-unknown}"
    echo "Base Name: $LFO_BASE_NAME"
    echo ""

    # Resource Group with portal link
    echo "✅ Resource Group: $LFO_RESOURCE_GROUP"
    if [ -n "${LFO_SUBSCRIPTION_ID:-}" ]; then
        echo "   🌐 https://portal.azure.com/#@/resource/subscriptions/${LFO_SUBSCRIPTION_ID}/resourceGroups/${LFO_RESOURCE_GROUP}/overview"
    fi

    # Container Registry
    echo ""
    echo "📦 Container Registry:"
    if [ -n "${LFO_CONTAINER_REGISTRY:-}" ]; then
        echo "   Name: $LFO_CONTAINER_REGISTRY"
        echo "   Login Server: ${LFO_CONTAINER_REGISTRY}.azurecr.io"
    else
        echo "   ❌ No container registry found"
    fi

    # Function Apps
    echo ""
    echo "⚡ Function Apps (${LFO_FUNCTION_APPS:+$(echo "$LFO_FUNCTION_APPS" | wc -l | tr -d ' ')} found):"
    if [ -n "${LFO_FUNCTION_APPS:-}" ]; then
        while IFS= read -r app; do
            [ -z "$app" ] && continue
            echo "   - $app"
        done <<< "$LFO_FUNCTION_APPS"
    else
        echo "   ❌ No function apps found"
    fi

    # Loggy details
    echo ""
    echo "🧪 Loggy (Test Log Generator):"
    if [ -n "${LFO_FUNCTION_APP:-}" ]; then
        echo "   Name: $LFO_FUNCTION_APP"
        echo "   URL: https://${LFO_FUNCTION_APP}.azurewebsites.net"
        if [ -n "${LFO_FUNCTION_KEY:-}" ]; then
            echo "   Function Key: ${LFO_FUNCTION_KEY:0:10}..."
        fi
    else
        echo "   ❌ No Loggy function app found"
    fi

    # Storage Account
    echo ""
    echo "💾 Storage Account:"
    if [ -n "${LFO_STORAGE_ACCOUNT:-}" ]; then
        echo "   Name: $LFO_STORAGE_ACCOUNT"
        if [ -n "${LFO_SUBSCRIPTION_ID:-}" ]; then
            echo "   🌐 https://portal.azure.com/#@/resource/subscriptions/${LFO_SUBSCRIPTION_ID}/resourceGroups/${LFO_RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${LFO_STORAGE_ACCOUNT}/overview"
        fi
        if [ -n "${LFO_STORAGE_CONNECTION_STRING:-}" ]; then
            echo "   Connection: ...${LFO_STORAGE_CONNECTION_STRING: -20}"
        fi
    else
        echo "   ❌ No storage accounts found"
    fi

    # Environment variables summary
    echo ""
    echo "📝 Environment Variables (add to ~/.profile or use --export):"
    echo "   export LFO_ENV_TYPE=\"lfo\""
    echo "   export LFO_BASE_NAME=\"$LFO_BASE_NAME\""
    echo "   export LFO_RESOURCE_GROUP=\"$LFO_RESOURCE_GROUP\""
    echo "   export LFO_FUNCTION_APP=\"${LFO_FUNCTION_APP:-NOT_FOUND}\""
    echo "   export LFO_FUNCTION_KEY=\"${LFO_FUNCTION_KEY:-NOT_FOUND}\""
    echo "   export LFO_STORAGE_ACCOUNT=\"${LFO_STORAGE_ACCOUNT:-NOT_FOUND}\""
    echo "   export LFO_CONTAINER_REGISTRY=\"${LFO_CONTAINER_REGISTRY:-NOT_FOUND}\""

    echo ""
    echo "💡 Tip: Run '$0 --export >> ~/.profile' to save these permanently"

elif [ "$LFO_ENV_TYPE" = "forwarder" ]; then
    # ========== Forwarder (VM) Environment ==========
    echo "🔍 Discovering Personal Forwarder Environment"
    echo "=============================================="
    echo "User: ${USER:-unknown}"
    echo "Base Name: $LFO_VM_BASE_NAME"
    echo ""
    echo "✅ Resource Group: $LFO_RESOURCE_GROUP"

    # VM details
    echo ""
    echo "🖥️  Virtual Machine:"
    if [ -n "${LFO_VM_NAME:-}" ]; then
        echo "   Name: $LFO_VM_NAME"
        echo "   Public IP: ${LFO_VM_IP:-Not found}"

        # Check SSH connectivity
        if [ -n "${LFO_VM_IP:-}" ]; then
            ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} "echo '   SSH Access: ✅ Connected'" 2>/dev/null || echo "   SSH Access: ❌ Cannot connect"
        fi
    else
        echo "   ❌ No VM found in resource group"
    fi

    # Function App details
    echo ""
    echo "⚡ Function App:"
    if [ -n "${LFO_FUNCTION_APP:-}" ]; then
        echo "   Name: $LFO_FUNCTION_APP"
        echo "   URL: https://${LFO_FUNCTION_APP}.azurewebsites.net"
        if [ -n "${LFO_FUNCTION_KEY:-}" ]; then
            echo "   Function Key: ${LFO_FUNCTION_KEY:0:10}..."
        fi
    else
        echo "   ❌ No function apps found"
    fi

    # Storage Account details
    echo ""
    echo "💾 Storage Account:"
    if [ -n "${LFO_STORAGE_ACCOUNT:-}" ]; then
        echo "   Name: $LFO_STORAGE_ACCOUNT"
        if [ -n "${LFO_STORAGE_CONNECTION_STRING:-}" ]; then
            echo "   Connection: ...${LFO_STORAGE_CONNECTION_STRING: -20}"
        fi
    else
        echo "   ❌ No storage accounts found"
    fi

    # Check forwarder configuration on VM
    if [ -n "${LFO_VM_IP:-}" ]; then
        echo ""
        echo "⚙️  Forwarder Configuration:"
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
            "sudo cat /etc/datadog-forwarder/environment 2>/dev/null | grep -E '^(DD_SITE|DD_TELEMETRY|VERSION_TAG)'" 2>/dev/null || echo "   Unable to retrieve configuration"
    fi

    # Environment variables summary
    echo ""
    echo "📝 Environment Variables (add to ~/.profile or use --export):"
    echo "   export LFO_ENV_TYPE=\"forwarder\""
    echo "   export LFO_VM_BASE_NAME=\"$LFO_VM_BASE_NAME\""
    echo "   export LFO_VM_IP=\"${LFO_VM_IP:-NOT_FOUND}\""
    echo "   export LFO_FUNCTION_APP=\"${LFO_FUNCTION_APP:-NOT_FOUND}\""
    echo "   export LFO_FUNCTION_KEY=\"${LFO_FUNCTION_KEY:-NOT_FOUND}\""
    echo "   export LFO_RESOURCE_GROUP=\"$LFO_RESOURCE_GROUP\""
    echo "   export LFO_STORAGE_ACCOUNT=\"${LFO_STORAGE_ACCOUNT:-NOT_FOUND}\""

    echo ""
    echo "💡 Tip: Run '$0 --export >> ~/.profile' to save these permanently"

else
    # ========== No environment found ==========
    echo "❌ No LFO or forwarder environment found!"
    echo ""
    echo "Available resource groups containing 'lfo' or your username:"
    az group list --query "[?contains(name, 'lfo') || contains(name, '${USER}')].name" -o tsv 2>/dev/null || echo "   (none found)"
    echo ""
    echo "To deploy your environment:"
    echo "  /deploy lfo    - Deploy an LFO (function app) environment"
    echo "  /deploy         - Deploy a VM forwarder environment"
    echo ""
    echo "Or set LFO_BASE_NAME / LFO_VM_BASE_NAME to match your deployed resources."
    exit 1
fi
