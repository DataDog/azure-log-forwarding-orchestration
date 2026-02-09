# View Forwarder Logs

View and analyze forwarder logs from the Azure VM.

## Usage
Use this skill to check forwarder execution logs, debug issues, and monitor processing statistics.

## Parameters
- `LINES`: Number of log lines to show (default: 30)
- `FOLLOW`: Follow log output in real-time (default: false)
- `FILTER`: Filter pattern for logs (optional)

## Implementation

```bash
# Source common discovery functions
SCRIPT_DIR="$(dirname "$0")"
source "${SCRIPT_DIR}/common-discovery.sh"

# Discover resources
echo "🔍 Discovering Azure resources..."
if ! discover_resources; then
    echo "❌ Failed to discover resources. Please run 'discover-environment' skill first."
    exit 1
fi

# Configuration from discovered resources
VM_IP="${LFO_VM_IP}"

# Validate we have the VM IP
if [ -z "$VM_IP" ]; then
    echo "❌ VM IP not found. Please ensure VM is deployed and running."
    exit 1
fi

# Parameters with defaults
LINES="${LINES:-30}"
FOLLOW="${FOLLOW:-false}"
FILTER="${FILTER:-}"

echo "📋 Forwarder Logs from VM ($VM_IP)"
echo "=================================="

# Show recent logs
if [ "$FOLLOW" = "true" ]; then
    echo "Following logs in real-time (Ctrl+C to stop)..."
    ssh -o StrictHostKeyChecking=no azureuser@${VM_IP} \
        "sudo journalctl -u datadog-forwarder -f"
else
    if [ -n "$FILTER" ]; then
        echo "Showing last $LINES lines matching: $FILTER"
        ssh -o StrictHostKeyChecking=no azureuser@${VM_IP} \
            "sudo journalctl -u datadog-forwarder -n $LINES --no-pager | grep -i '$FILTER'"
    else
        echo "Showing last $LINES lines:"
        ssh -o StrictHostKeyChecking=no azureuser@${VM_IP} \
            "sudo journalctl -u datadog-forwarder -n $LINES --no-pager"
    fi
fi

echo ""
echo "📊 Processing Statistics (last 10 runs):"
echo "-----------------------------------------"
ssh -o StrictHostKeyChecking=no azureuser@${VM_IP} \
    "sudo journalctl -u datadog-forwarder --no-pager | grep 'Finished processing' | tail -10"

echo ""
echo "⚠️  Recent Errors (if any):"
echo "----------------------------"
ssh -o StrictHostKeyChecking=no azureuser@${VM_IP} \
    "sudo journalctl -u datadog-forwarder -p err -n 10 --no-pager" 2>/dev/null || echo "No errors found"

echo ""
echo "🕐 Last Execution Times:"
echo "------------------------"
ssh -o StrictHostKeyChecking=no azureuser@${VM_IP} \
    "sudo systemctl status datadog-forwarder.timer --no-pager | grep -E 'Trigger|Active'"
```

## Examples

```bash
# View last 50 lines
LINES=50 ./view-forwarder-logs.md

# Follow logs in real-time
FOLLOW=true ./view-forwarder-logs.md

# Filter for specific pattern
FILTER="error" LINES=100 ./view-forwarder-logs.md

# Check processing statistics only
FILTER="Finished processing" ./view-forwarder-logs.md
```

## Useful Filters
- `"Finished processing"` - Show processing summaries
- `"error"` - Show errors
- `"warning"` - Show warnings
- `"Start time"` - Show when runs started
- `"Run time"` - Show execution durations

## Notes
- The forwarder runs every minute via systemd timer
- Logs are managed by systemd journal
- Use `FOLLOW=true` for real-time monitoring during testing
- VM IP is automatically discovered based on your username or LFO_VM_BASE_NAME