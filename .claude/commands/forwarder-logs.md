---
name: forwarder-logs
description: View and analyze forwarder logs from VM
argument-hint: [--lines=N] [--follow] [--filter=pattern]
---

# View Forwarder Logs

View and analyze forwarder logs from the Azure VM.

## Usage
Use this command to check forwarder execution logs, debug issues, and monitor processing statistics.

## Implementation

```bash
#!/bin/bash

# Default values
LINES=30
FOLLOW=false
FILTER=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --lines=*)
            LINES="${1#*=}"
            shift
            ;;
        --follow)
            FOLLOW=true
            shift
            ;;
        --filter=*)
            FILTER="${1#*=}"
            shift
            ;;
        --help)
            echo "Usage: /forwarder-logs [--lines=N] [--follow] [--filter=pattern]"
            echo ""
            echo "Options:"
            echo "  --lines=N        Number of log lines to show (default: 30)"
            echo "  --follow         Follow log output in real-time"
            echo "  --filter=PATTERN Filter logs by pattern"
            echo ""
            echo "Examples:"
            echo "  /forwarder-logs --lines=50"
            echo "  /forwarder-logs --follow"
            echo "  /forwarder-logs --filter=error --lines=100"
            echo ""
            echo "Useful Filters:"
            echo "  'Finished processing' - Show processing summaries"
            echo "  'error'              - Show errors"
            echo "  'warning'            - Show warnings"
            echo "  'Start time'         - Show when runs started"
            echo "  'Run time'           - Show execution durations"
            exit 0
            ;;
        *)
            echo "Unknown argument: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Source common discovery functions
CLAUDE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${CLAUDE_DIR}/lib/azure-discovery.sh"

# Discover resources
echo "🔍 Discovering Azure resources..."
discover_resources 2>/dev/null
DISCOVERY_RESULT=$?

if [ $DISCOVERY_RESULT -ne 0 ]; then
    echo "❌ Failed to discover resources. Please run '/discover' first."
    exit 1
fi

# Validate we have the VM IP
if [ -z "$LFO_VM_IP" ]; then
    echo "❌ VM IP not found. Please ensure VM is deployed and running."
    echo "   Run '/deploy' to create your environment"
    exit 1
fi

echo "📋 Forwarder Logs from VM ($LFO_VM_IP)"
echo "=================================="

# Show recent logs
if [ "$FOLLOW" = "true" ]; then
    echo "Following logs in real-time (Ctrl+C to stop)..."
    ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
        "sudo journalctl -u datadog-forwarder -f"
else
    if [ -n "$FILTER" ]; then
        echo "Showing last $LINES lines matching: $FILTER"
        ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
            "sudo journalctl -u datadog-forwarder -n $LINES --no-pager | grep -i '$FILTER'"
    else
        echo "Showing last $LINES lines:"
        ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
            "sudo journalctl -u datadog-forwarder -n $LINES --no-pager"
    fi
fi

# Only show statistics if not following
if [ "$FOLLOW" != "true" ]; then
    echo ""
    echo "📊 Processing Statistics (last 10 runs):"
    echo "-----------------------------------------"
    ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
        "sudo journalctl -u datadog-forwarder --no-pager | grep 'Finished processing' | tail -10"

    echo ""
    echo "⚠️  Recent Errors (if any):"
    echo "----------------------------"
    ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
        "sudo journalctl -u datadog-forwarder -p err -n 10 --no-pager" 2>/dev/null || echo "No errors found"

    echo ""
    echo "🕐 Last Execution Times:"
    echo "------------------------"
    ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
        "sudo systemctl status datadog-forwarder.timer --no-pager | grep -E 'Trigger|Active'"
fi
```

## Examples

```bash
# View last 50 lines
/forwarder-logs --lines=50

# Follow logs in real-time
/forwarder-logs --follow

# Filter for specific pattern
/forwarder-logs --filter=error --lines=100

# Check processing statistics only
/forwarder-logs --filter="Finished processing"
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
- Use `--follow` for real-time monitoring during testing
- VM IP is automatically discovered based on your username or LFO_VM_BASE_NAME
