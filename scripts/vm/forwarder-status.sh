#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

usage() {
    echo "Usage: $0 [--errors-only]"
    echo "       /forwarder-status [--errors-only]"
    echo ""
    echo "Check comprehensive forwarder status and health."
    echo ""
    echo "Options:"
    echo "  --errors-only    Only show errors and problems"
    echo "  --help           Show this help message"
}

# Parse arguments
ERRORS_ONLY=false
for arg in "$@"; do
    case $arg in
        --errors-only)
            ERRORS_ONLY=true
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

# Source common discovery functions
source "${REPO_ROOT}/scripts/lib/azure-discovery.sh"

# Discover resources
discover_resources 2>/dev/null
DISCOVERY_RESULT=$?

if [ $DISCOVERY_RESULT -ne 0 ]; then
    echo "❌ Failed to discover resources. Run '/discover' or scripts/vm/discover.sh first"
    exit 1
fi

# Check if we have a VM IP
if [ -z "$LFO_VM_IP" ]; then
    echo "❌ No VM IP found. Have you deployed your environment?"
    echo "   Run '/deploy' or scripts/vm/deploy.sh to create your environment"
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
    SERVICE_STATUS=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
        "sudo systemctl is-active datadog-forwarder.timer" 2>/dev/null)

    if [ "$SERVICE_STATUS" != "active" ]; then
        echo "⚠️  Timer is not active: $SERVICE_STATUS"
    else
        echo "✅ Timer is active"
    fi

    # Check for recent errors
    ERROR_COUNT=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
        "sudo journalctl -u datadog-forwarder -p err --since '1 hour ago' --no-pager | wc -l" 2>/dev/null)

    if [ "$ERROR_COUNT" -gt 0 ]; then
        echo ""
        echo "❌ Found $ERROR_COUNT errors in the last hour:"
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
            "sudo journalctl -u datadog-forwarder -p err --since '1 hour ago' --no-pager"
    else
        echo "✅ No errors in the last hour"
    fi

    # Check for warnings
    WARN_COUNT=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
        "sudo journalctl -u datadog-forwarder -p warning --since '1 hour ago' --no-pager | wc -l" 2>/dev/null)

    if [ "$WARN_COUNT" -gt 0 ]; then
        echo ""
        echo "⚠️  Found $WARN_COUNT warnings in the last hour:"
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
            "sudo journalctl -u datadog-forwarder -p warning --since '1 hour ago' --no-pager | tail -10"
    fi

    # Check Datadog Agent if installed
    AGENT_INSTALLED=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
        "command -v datadog-agent" 2>/dev/null)

    if [ ! -z "$AGENT_INSTALLED" ]; then
        echo ""
        AGENT_STATUS=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
            "sudo systemctl is-active datadog-agent" 2>/dev/null)

        if [ "$AGENT_STATUS" != "active" ]; then
            echo "⚠️  Datadog Agent is not active: $AGENT_STATUS"
        else
            echo "✅ Datadog Agent is active"
        fi
    fi

    exit 0
fi

# Full status report
echo "⏲️  Timer Status:"
ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
    "sudo systemctl status datadog-forwarder.timer --no-pager | head -15"

echo ""
echo "🔧 Service Status:"
ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
    "sudo systemctl status datadog-forwarder.service --no-pager | head -10"

echo ""
echo "📝 Environment Configuration:"
ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
    "sudo cat /etc/datadog-forwarder/environment | grep -E '^(DD_SITE|DD_TELEMETRY|VERSION_TAG|NUM_GOROUTINES)'"

echo ""
echo "📈 Recent Processing (last 5 runs):"
ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
    "sudo journalctl -u datadog-forwarder --no-pager | grep 'Finished processing' | tail -5"

echo ""
echo "⚠️  Recent Errors (if any):"
ERROR_COUNT=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
    "sudo journalctl -u datadog-forwarder -p err --since '1 hour ago' --no-pager | wc -l" 2>/dev/null)
if [ "$ERROR_COUNT" -gt 0 ]; then
    echo "Found $ERROR_COUNT errors in the last hour:"
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
        "sudo journalctl -u datadog-forwarder -p err --since '1 hour ago' --no-pager | tail -10"
else
    echo "✅ No errors in the last hour"
fi

echo ""
echo "💾 Blob Processing:"
ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
    "sudo journalctl -u datadog-forwarder --no-pager | grep -E 'processing blob|container' | tail -5"

# Check Datadog Agent status (if installed)
echo ""
echo "🐶 Datadog Agent Status:"
AGENT_INSTALLED=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
    "command -v datadog-agent" 2>/dev/null)

if [ ! -z "$AGENT_INSTALLED" ]; then
    AGENT_STATUS=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
        "sudo systemctl is-active datadog-agent" 2>/dev/null)

    if [ "$AGENT_STATUS" = "active" ]; then
        echo "✅ Agent is running"

        # Get agent version
        AGENT_VERSION=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
            "sudo datadog-agent version 2>/dev/null | grep 'Agent' | head -1" 2>/dev/null)
        if [ ! -z "$AGENT_VERSION" ]; then
            echo "   Version: $AGENT_VERSION"
        fi

        # Check agent connectivity
        AGENT_HEALTH=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
            "sudo datadog-agent health 2>/dev/null | grep -E '(API|Forwarder)' | head -2" 2>/dev/null)
        if [ ! -z "$AGENT_HEALTH" ]; then
            echo "   Health:"
            echo "$AGENT_HEALTH" | sed 's/^/     /'
        fi

        # Check APM status
        APM_STATUS=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
            "sudo datadog-agent status 2>/dev/null | grep -A 2 'APM Agent' | tail -2" 2>/dev/null)
        if [ ! -z "$APM_STATUS" ]; then
            echo "   APM Receiver: Listening on localhost:8126"
        fi

        # Show collected metrics count
        METRICS_COUNT=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} \
            "sudo datadog-agent status 2>/dev/null | grep 'Metrics' | grep -oE '[0-9]+' | head -1" 2>/dev/null)
        if [ ! -z "$METRICS_COUNT" ]; then
            echo "   Metrics collected: $METRICS_COUNT"
        fi
    else
        echo "⚠️  Agent installed but not running (status: $AGENT_STATUS)"
        echo "   To start: ssh azureuser@${LFO_VM_IP} 'sudo systemctl start datadog-agent'"
    fi
else
    echo "ℹ️  Agent not installed"
    echo "   To install: Re-deploy (agent is installed by default)"
    echo "   Note: Use --skip-agent flag only if you don't want the agent"
fi

echo ""
echo "🔗 Datadog Links:"
echo "   Logs: https://app.datadoghq.com/logs?query=service%3Aazure-log-forwarder"
echo "   Search: https://app.datadoghq.com/logs?query=%40azure.resource_name%3A${LFO_VM_BASE_NAME}*"
if [ ! -z "$AGENT_INSTALLED" ] && [ "$AGENT_STATUS" = "active" ]; then
    echo "   Infrastructure: https://app.datadoghq.com/infrastructure?host=${LFO_VM_BASE_NAME}"
    echo "   Processes: https://app.datadoghq.com/process?hostname=${LFO_VM_BASE_NAME}"
fi
