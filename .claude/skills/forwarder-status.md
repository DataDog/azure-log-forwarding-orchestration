# Check Forwarder Status

Check the status and health of your personal forwarder deployment.

## Usage
This skill provides a comprehensive view of your forwarder's status, including service state, recent logs, and processing statistics.

## Implementation

```bash
#!/bin/bash

# Discover environment
USERNAME="${USER:-unknown}"
BASE_NAME="${LFO_VM_BASE_NAME:-lfo${USERNAME}vm}"
RESOURCE_GROUP="${BASE_NAME}rg"

# Get VM IP
if [ -z "$LFO_VM_IP" ]; then
    echo "🔍 Discovering VM..."
    VM_NAME=$(az vm list --resource-group "$RESOURCE_GROUP" --query "[0].name" -o tsv 2>/dev/null)
    if [ -z "$VM_NAME" ]; then
        echo "❌ No VM found. Run 'discover-environment' skill first"
        exit 1
    fi
    VM_IP=$(az vm list-ip-addresses --resource-group "$RESOURCE_GROUP" --name "$VM_NAME" \
            --query "[0].virtualMachine.network.publicIpAddresses[0].ipAddress" -o tsv)
else
    VM_IP="$LFO_VM_IP"
fi

echo "📊 Forwarder Status Report"
echo "=========================="
echo "VM: $VM_IP"
echo ""

# Check timer status
echo "⏲️  Timer Status:"
ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${VM_IP} \
    "sudo systemctl status datadog-forwarder.timer --no-pager | head -15"

echo ""
echo "🔧 Service Status:"
ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${VM_IP} \
    "sudo systemctl status datadog-forwarder.service --no-pager | head -10"

echo ""
echo "📝 Environment Configuration:"
ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${VM_IP} \
    "sudo cat /etc/datadog-forwarder/environment | grep -E '^(DD_SITE|DD_TELEMETRY|VERSION_TAG|NUM_GOROUTINES)'"

echo ""
echo "📈 Recent Processing (last 5 runs):"
ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${VM_IP} \
    "sudo journalctl -u datadog-forwarder --no-pager | grep 'Finished processing' | tail -5"

echo ""
echo "⚠️  Recent Errors (if any):"
ERROR_COUNT=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${VM_IP} \
    "sudo journalctl -u datadog-forwarder -p err --since '1 hour ago' --no-pager | wc -l" 2>/dev/null)
if [ "$ERROR_COUNT" -gt 0 ]; then
    echo "Found $ERROR_COUNT errors in the last hour:"
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${VM_IP} \
        "sudo journalctl -u datadog-forwarder -p err --since '1 hour ago' --no-pager | tail -10"
else
    echo "✅ No errors in the last hour"
fi

echo ""
echo "💾 Blob Processing:"
ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${VM_IP} \
    "sudo journalctl -u datadog-forwarder --no-pager | grep -E 'processing blob|container' | tail -5"

echo ""
echo "🔗 Datadog Links:"
echo "   Logs: https://app.datadoghq.com/logs?query=service%3Aazure-log-forwarder"
echo "   Search: https://app.datadoghq.com/logs?query=%40azure.resource_name%3A${BASE_NAME}*"
```

## Notes
- Requires SSH access to the VM
- Shows both timer and service status
- Displays environment configuration
- Shows recent processing statistics and errors