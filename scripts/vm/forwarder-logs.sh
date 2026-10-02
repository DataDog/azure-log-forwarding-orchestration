#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

usage() {
    echo "Usage: $0 [--lines=N] [--follow] [--filter=pattern]"
    echo "       /forwarder-logs [--lines=N] [--follow] [--filter=pattern]"
    echo ""
    echo "View and analyze forwarder logs from VM."
    echo ""
    echo "Options:"
    echo "  --lines=N        Number of log lines to show (default: 30)"
    echo "  --follow         Follow log output in real-time"
    echo "  --filter=PATTERN Filter logs by pattern"
    echo "  --help           Show this help message"
    echo ""
    echo "Useful Filters:"
    echo "  'Finished processing' - Show processing summaries"
    echo "  'error'              - Show errors"
    echo "  'warning'            - Show warnings"
    echo "  'Start time'         - Show when runs started"
    echo "  'Run time'           - Show execution durations"
}

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
        --help|-h)
            usage
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
source "${REPO_ROOT}/scripts/vm/lib/azure-discovery.sh"

# Discover resources
echo "🔍 Discovering Azure resources..."
discover_resources 2>/dev/null
DISCOVERY_RESULT=$?

if [ $DISCOVERY_RESULT -ne 0 ]; then
    echo "❌ Failed to discover resources. Please run '/discover' or scripts/vm/discover.sh first."
    exit 1
fi

# Validate we have the VM IP
if [ -z "$LFO_VM_IP" ]; then
    echo "❌ VM IP not found. Please ensure VM is deployed and running."
    echo "   Run '/deploy' or scripts/vm/deploy.sh to create your environment"
    exit 1
fi

echo "📋 Forwarder Logs from VM ($LFO_VM_IP)"
echo "=================================="

# Show recent logs
if [ "$FOLLOW" = "true" ]; then
    echo "Following logs in real-time (Ctrl+C to stop)..."
    ssh -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
        "sudo journalctl -u datadog-forwarder -f"
else
    if [ -n "$FILTER" ]; then
        echo "Showing last $LINES lines matching: $FILTER"
        ssh -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
            "sudo journalctl -u datadog-forwarder -n $LINES --no-pager | grep -Fi '$FILTER'"
    else
        echo "Showing last $LINES lines:"
        ssh -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
            "sudo journalctl -u datadog-forwarder -n $LINES --no-pager"
    fi
fi

# Only show statistics if not following
if [ "$FOLLOW" != "true" ]; then
    echo ""
    echo "📊 Processing Statistics (last 10 runs):"
    echo "-----------------------------------------"
    ssh -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
        "sudo journalctl -u datadog-forwarder --no-pager | grep 'Finished processing' | tail -10"

    echo ""
    echo "⚠️  Recent Errors (if any):"
    echo "----------------------------"
    ssh -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
        "sudo journalctl -u datadog-forwarder -p err -n 10 --no-pager" 2>/dev/null || echo "No errors found"

    echo ""
    echo "🕐 Last Execution Times:"
    echo "------------------------"
    ssh -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
        "sudo systemctl status datadog-forwarder.timer --no-pager | grep -E 'Trigger|Active'"
fi
