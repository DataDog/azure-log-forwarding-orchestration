---
name: forwarder-status
description: Check comprehensive forwarder status and health
argument-hint: [--errors-only]
---

# Check Forwarder Status

Check the status and health of your personal forwarder deployment.

## Usage
This command provides a comprehensive view of your forwarder's status, including service state, recent logs, and processing statistics.

## Implementation

```bash
#!/bin/bash

# Parse arguments
ERRORS_ONLY=false
for arg in "$@"; do
    case $arg in
        --errors-only)
            ERRORS_ONLY=true
            shift
            ;;
        --help)
            echo "Usage: /forwarder-status [--errors-only]"
            echo ""
            echo "Options:"
            echo "  --errors-only    Only show errors and problems"
            echo ""
            echo "This command checks the status of your forwarder deployment,"
            echo "including service state, recent logs, and processing statistics."
            exit 0
            ;;
        *)
            ;;
    esac
done

# Source common discovery functions
CLAUDE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${CLAUDE_DIR}/lib/azure-discovery.sh"

# Discover resources
discover_resources 2>/dev/null
DISCOVERY_RESULT=$?

if [ $DISCOVERY_RESULT -ne 0 ]; then
    echo "❌ Failed to discover resources. Run '/discover' first"
    exit 1
fi

# Check if we have a VM IP
if [ -z "$LFO_VM_IP" ]; then
    echo "❌ No VM IP found. Have you deployed your environment?"
    echo "   Run '/deploy' to create your environment"
    exit 1
fi

echo "📊 Forwarder Status Report"
echo "=========================="
echo "VM: $LFO_VM_IP"
echo "Resource Group: $LFO_RESOURCE_GROUP"
echo ""

# If errors only mode, just show errors and critical info
if [ "$ERRORS_ONLY" = "true" ]; then
    echo "🔍 Checking for errors and issues..."
    echo ""

    # Check if service is active
    SERVICE_STATUS=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
        "sudo systemctl is-active datadog-forwarder.timer" 2>/dev/null)

    if [ "$SERVICE_STATUS" != "active" ]; then
        echo "⚠️  Timer is not active: $SERVICE_STATUS"
    else
        echo "✅ Timer is active"
    fi

    # Check for recent errors
    ERROR_COUNT=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
        "sudo journalctl -u datadog-forwarder -p err --since '1 hour ago' --no-pager | wc -l" 2>/dev/null)

    if [ "$ERROR_COUNT" -gt 0 ]; then
        echo ""
        echo "❌ Found $ERROR_COUNT errors in the last hour:"
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
            "sudo journalctl -u datadog-forwarder -p err --since '1 hour ago' --no-pager"
    else
        echo "✅ No errors in the last hour"
    fi

    # Check for warnings
    WARN_COUNT=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
        "sudo journalctl -u datadog-forwarder -p warning --since '1 hour ago' --no-pager | wc -l" 2>/dev/null)

    if [ "$WARN_COUNT" -gt 0 ]; then
        echo ""
        echo "⚠️  Found $WARN_COUNT warnings in the last hour:"
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
            "sudo journalctl -u datadog-forwarder -p warning --since '1 hour ago' --no-pager | tail -10"
    fi

    exit 0
fi

# Full status report
echo "⏲️  Timer Status:"
ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
    "sudo systemctl status datadog-forwarder.timer --no-pager | head -15"

echo ""
echo "🔧 Service Status:"
ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
    "sudo systemctl status datadog-forwarder.service --no-pager | head -10"

echo ""
echo "📝 Environment Configuration:"
ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
    "sudo cat /etc/datadog-forwarder/environment | grep -E '^(DD_SITE|DD_TELEMETRY|VERSION_TAG|NUM_GOROUTINES)'"

echo ""
echo "📈 Recent Processing (last 5 runs):"
ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
    "sudo journalctl -u datadog-forwarder --no-pager | grep 'Finished processing' | tail -5"

echo ""
echo "⚠️  Recent Errors (if any):"
ERROR_COUNT=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
    "sudo journalctl -u datadog-forwarder -p err --since '1 hour ago' --no-pager | wc -l" 2>/dev/null)
if [ "$ERROR_COUNT" -gt 0 ]; then
    echo "Found $ERROR_COUNT errors in the last hour:"
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
        "sudo journalctl -u datadog-forwarder -p err --since '1 hour ago' --no-pager | tail -10"
else
    echo "✅ No errors in the last hour"
fi

echo ""
echo "💾 Blob Processing:"
ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
    "sudo journalctl -u datadog-forwarder --no-pager | grep -E 'processing blob|container' | tail -5"

echo ""
echo "🔗 Datadog Links:"
echo "   Logs: https://app.datadoghq.com/logs?query=service%3Aazure-log-forwarder"
echo "   Search: https://app.datadoghq.com/logs?query=%40azure.resource_name%3A${LFO_VM_BASE_NAME}*"
```

## Examples

```bash
# Full status report
/forwarder-status

# Only show errors and issues
/forwarder-status --errors-only
```

## Notes
- Requires SSH access to the VM
- Shows both timer and service status
- Displays environment configuration
- Shows recent processing statistics and errors
- Use --errors-only for a quick health check
