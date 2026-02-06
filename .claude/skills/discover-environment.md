# Discover Personal Environment

Discover and display your personal forwarder environment resources.

## Usage
This skill discovers your Azure resources based on your username and environment variables. Run this first to find your VM IP, function app name, and other resources.

## Implementation

```bash
#!/bin/bash

# Get user's environment
USERNAME="${USER:-unknown}"
BASE_NAME="${LFO_VM_BASE_NAME:-lfo${USERNAME}vm}"
RESOURCE_GROUP="${BASE_NAME}rg"

echo "🔍 Discovering Personal Forwarder Environment"
echo "=============================================="
echo "User: $USERNAME"
echo "Base Name: $BASE_NAME"
echo ""

# Check if resource group exists
RG_EXISTS=$(az group exists --name "$RESOURCE_GROUP" 2>/dev/null)

if [ "$RG_EXISTS" != "true" ]; then
    echo "❌ Resource group $RESOURCE_GROUP not found!"
    echo ""
    echo "Available resource groups containing 'lfo':"
    az group list --query "[?contains(name, 'lfo')].name" -o tsv
    echo ""
    echo "To deploy your environment, run: skill deploy-personal-env"
    exit 1
fi

echo "✅ Resource Group: $RESOURCE_GROUP"

# Get VM details
echo ""
echo "🖥️  Virtual Machine:"
VM_INFO=$(az vm list --resource-group "$RESOURCE_GROUP" --query "[0]" -o json 2>/dev/null || echo "{}")
if [ "$VM_INFO" != "{}" ]; then
    VM_NAME=$(echo "$VM_INFO" | jq -r '.name')
    echo "   Name: $VM_NAME"

    # Get public IP
    VM_IP=$(az vm list-ip-addresses --resource-group "$RESOURCE_GROUP" --name "$VM_NAME" \
            --query "[0].virtualMachine.network.publicIpAddresses[0].ipAddress" -o tsv 2>/dev/null)
    echo "   Public IP: ${VM_IP:-Not found}"

    # Check SSH connectivity
    if [ -n "$VM_IP" ]; then
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${VM_IP} "echo '   SSH Access: ✅ Connected'" 2>/dev/null || echo "   SSH Access: ❌ Cannot connect"
    fi
else
    echo "   ❌ No VM found in resource group"
fi

# Get Function App details
echo ""
echo "⚡ Function App:"
FUNCTION_APPS=$(az functionapp list --resource-group "$RESOURCE_GROUP" --query "[].name" -o tsv 2>/dev/null)
if [ -n "$FUNCTION_APPS" ]; then
    for APP in $FUNCTION_APPS; do
        echo "   Name: $APP"
        echo "   URL: https://${APP}.azurewebsites.net"

        # Try to get function key
        FUNCTION_KEY=$(az functionapp function keys list \
                      --resource-group "$RESOURCE_GROUP" \
                      --name "$APP" \
                      --function-name "CustomLog" \
                      --query "default" -o tsv 2>/dev/null || echo "")
        if [ -n "$FUNCTION_KEY" ]; then
            echo "   Function Key: ${FUNCTION_KEY:0:10}..."
        fi
    done
else
    echo "   ❌ No function apps found"
fi

# Get Storage Account details
echo ""
echo "💾 Storage Account:"
STORAGE_ACCOUNTS=$(az storage account list --resource-group "$RESOURCE_GROUP" --query "[].name" -o tsv 2>/dev/null)
if [ -n "$STORAGE_ACCOUNTS" ]; then
    for STORAGE in $STORAGE_ACCOUNTS; do
        echo "   Name: $STORAGE"

        # Get connection string (truncated for security)
        CONN_STR=$(az storage account show-connection-string \
                   --resource-group "$RESOURCE_GROUP" \
                   --name "$STORAGE" \
                   --query "connectionString" -o tsv 2>/dev/null || echo "")
        if [ -n "$CONN_STR" ]; then
            echo "   Connection: ...${CONN_STR: -20}"
        fi
    done
else
    echo "   ❌ No storage accounts found"
fi

# Check forwarder configuration on VM
if [ -n "$VM_IP" ]; then
    echo ""
    echo "⚙️  Forwarder Configuration:"
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${VM_IP} \
        "sudo cat /etc/datadog-forwarder/environment 2>/dev/null | grep -E '^(DD_SITE|DD_TELEMETRY|VERSION_TAG)'" 2>/dev/null || echo "   Unable to retrieve configuration"
fi

# Export discovered values for use in other scripts
echo ""
echo "📝 Environment Variables (add to ~/.profile or export):"
echo "   export LFO_VM_BASE_NAME=\"$BASE_NAME\""
echo "   export LFO_VM_IP=\"${VM_IP:-NOT_FOUND}\""
echo "   export LFO_FUNCTION_APP=\"${APP:-NOT_FOUND}\""
echo "   export LFO_FUNCTION_KEY=\"${FUNCTION_KEY:-NOT_FOUND}\""
echo "   export LFO_RESOURCE_GROUP=\"$RESOURCE_GROUP\""
```

## Notes
- This discovers resources based on your username
- Set LFO_VM_BASE_NAME to override the default naming
- Exports can be saved to ~/.profile for persistence
- Run this before using other forwarder skills