---
name: discover
description: Discover and display your Azure resources
argument-hint: [--export]
---

# Discover Personal Environment

Discover and display your personal forwarder environment resources.

## Usage
This command discovers your Azure resources based on your username and environment variables. Run this first to find your VM IP, function app name, and other resources.

## Implementation

```bash
#!/bin/bash

# Parse arguments
EXPORT_MODE=false
for arg in "$@"; do
    case $arg in
        --export)
            EXPORT_MODE=true
            shift
            ;;
        --help)
            echo "Usage: /discover [--export]"
            echo ""
            echo "Options:"
            echo "  --export    Output environment variables for export"
            echo ""
            echo "This command discovers your Azure resources based on your username."
            echo "It will find your VM, Function App, Storage Account, and other resources."
            exit 0
            ;;
        *)
            ;;
    esac
done

# Source common discovery functions
CLAUDE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${CLAUDE_DIR}/lib/azure-discovery.sh"

if [ "$EXPORT_MODE" = "true" ]; then
    # Export mode - just output the variables
    if discover_resources 2>/dev/null; then
        echo "export LFO_VM_BASE_NAME=\"$LFO_VM_BASE_NAME\""
        echo "export LFO_VM_IP=\"${LFO_VM_IP:-}\""
        echo "export LFO_VM_NAME=\"${LFO_VM_NAME:-}\""
        echo "export LFO_FUNCTION_APP=\"${LFO_FUNCTION_APP:-}\""
        echo "export LFO_FUNCTION_KEY=\"${LFO_FUNCTION_KEY:-}\""
        echo "export LFO_RESOURCE_GROUP=\"$LFO_RESOURCE_GROUP\""
        echo "export LFO_STORAGE_ACCOUNT=\"${LFO_STORAGE_ACCOUNT:-}\""
    fi
    exit 0
fi

echo "🔍 Discovering Personal Forwarder Environment"
echo "=============================================="

# Discover resources
discover_resources
DISCOVERY_RESULT=$?

if [ $DISCOVERY_RESULT -ne 0 ]; then
    echo "❌ Failed to discover resources!"
    echo ""
    echo "Available resource groups containing 'lfo' or your username:"
    az group list --query "[?contains(name, 'lfo') || contains(name, '${USER}')].name" -o tsv
    echo ""
    echo "To deploy your environment, run: /deploy"
    echo "Or set LFO_VM_BASE_NAME to match your deployed resources"
    exit 1
fi

# Display discovered base configuration
echo "User: ${USER:-unknown}"
echo "Base Name: $LFO_VM_BASE_NAME"
echo ""
echo "✅ Resource Group: $LFO_RESOURCE_GROUP"

# Get VM details
echo ""
echo "🖥️  Virtual Machine:"
if [ -n "$LFO_VM_NAME" ]; then
    echo "   Name: $LFO_VM_NAME"
    echo "   Public IP: ${LFO_VM_IP:-Not found}"

    # Check SSH connectivity
    if [ -n "$LFO_VM_IP" ]; then
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} "echo '   SSH Access: ✅ Connected'" 2>/dev/null || echo "   SSH Access: ❌ Cannot connect"
    fi
else
    echo "   ❌ No VM found in resource group"
fi

# Get Function App details
echo ""
echo "⚡ Function App:"
if [ -n "$LFO_FUNCTION_APP" ]; then
    echo "   Name: $LFO_FUNCTION_APP"
    echo "   URL: https://${LFO_FUNCTION_APP}.azurewebsites.net"
    if [ -n "$LFO_FUNCTION_KEY" ]; then
        echo "   Function Key: ${LFO_FUNCTION_KEY:0:10}..."
    fi
else
    echo "   ❌ No function apps found"
fi

# Get Storage Account details
echo ""
echo "💾 Storage Account:"
if [ -n "$LFO_STORAGE_ACCOUNT" ]; then
    echo "   Name: $LFO_STORAGE_ACCOUNT"
    if [ -n "$LFO_STORAGE_CONNECTION_STRING" ]; then
        echo "   Connection: ...${LFO_STORAGE_CONNECTION_STRING: -20}"
    fi
else
    echo "   ❌ No storage accounts found"
fi

# Check forwarder configuration on VM
if [ -n "$LFO_VM_IP" ]; then
    echo ""
    echo "⚙️  Forwarder Configuration:"
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
        "sudo cat /etc/datadog-forwarder/environment 2>/dev/null | grep -E '^(DD_SITE|DD_TELEMETRY|VERSION_TAG)'" 2>/dev/null || echo "   Unable to retrieve configuration"
fi

# Export discovered values for use in other scripts
echo ""
echo "📝 Environment Variables (add to ~/.profile or use /discover --export):"
echo "   export LFO_VM_BASE_NAME=\"$LFO_VM_BASE_NAME\""
echo "   export LFO_VM_IP=\"${LFO_VM_IP:-NOT_FOUND}\""
echo "   export LFO_FUNCTION_APP=\"${LFO_FUNCTION_APP:-NOT_FOUND}\""
echo "   export LFO_FUNCTION_KEY=\"${LFO_FUNCTION_KEY:-NOT_FOUND}\""
echo "   export LFO_RESOURCE_GROUP=\"$LFO_RESOURCE_GROUP\""
echo "   export LFO_STORAGE_ACCOUNT=\"${LFO_STORAGE_ACCOUNT:-NOT_FOUND}\""

echo ""
echo "💡 Tip: Run '/discover --export >> ~/.profile' to save these permanently"
```

## Examples

```bash
# Discover and display resources
/discover

# Output environment variables for export
/discover --export

# Save environment variables permanently
/discover --export >> ~/.profile
```

## Notes
- This discovers resources based on your username
- Set LFO_VM_BASE_NAME to override the default naming
- Exports can be saved to ~/.profile for persistence
- Run this before using other forwarder commands
