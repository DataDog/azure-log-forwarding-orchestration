#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

usage() {
    echo "Usage: $0 [--duration=30s] [--rps=10] [--variety] [--message=MSG] [--level=LEVEL] [--count=N]"
    echo "       /test-logs [--duration=30s] [--rps=10] [--variety]"
    echo ""
    echo "Generate test logs to Azure Function App."
    echo ""
    echo "Options:"
    echo "  --duration=TIME  How long to generate logs (default: 30s)"
    echo "  --rps=N          Requests per second (default: 10)"
    echo "  --variety        Use variety mode for fun messages"
    echo "  --message=MSG    Custom log message (when not using variety)"
    echo "  --level=LEVEL    Log level (info/warning/error, default: info)"
    echo "  --count=N        Number of log entries per request (default: 1)"
    echo "  --help           Show this help message"
}

# Default values
DURATION="30s"
RPS="10"
VARIETY=false
MESSAGE=""
LEVEL=""
COUNT=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --duration=*)
            DURATION="${1#*=}"
            shift
            ;;
        --rps=*)
            RPS="${1#*=}"
            shift
            ;;
        --variety)
            VARIETY=true
            shift
            ;;
        --message=*)
            MESSAGE="${1#*=}"
            shift
            ;;
        --level=*)
            LEVEL="${1#*=}"
            shift
            ;;
        --count=*)
            COUNT="${1#*=}"
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

# Validate we have the required resources
if [ -z "$LFO_FUNCTION_APP" ]; then
    echo "❌ Function app not found. Please deploy environment first."
    echo "   Run '/deploy' or scripts/vm/deploy.sh to create your environment"
    exit 1
fi

if [ -z "$LFO_FUNCTION_KEY" ]; then
    echo "⚠️  Function key not found. Trying to retrieve it..."
    LFO_FUNCTION_KEY=$(az functionapp function keys list \
        --resource-group "${LFO_RESOURCE_GROUP}" \
        --name "${LFO_FUNCTION_APP}" \
        --function-name "CustomLog" \
        --query "default" -o tsv 2>/dev/null || echo "")

    if [ -z "$LFO_FUNCTION_KEY" ]; then
        echo "❌ Could not retrieve function key. Please check Azure permissions."
        exit 1
    fi
fi

if [ -z "$LFO_VM_IP" ]; then
    echo "⚠️  VM IP not found. Forwarder trigger will be skipped."
fi

# Path to requesty
REQUESTY_PATH="${REPO_ROOT}/requesty"

# Build requesty if needed
if [ ! -f "$REQUESTY_PATH/requesty" ]; then
    echo "Building requesty..."
    cd "$REQUESTY_PATH"
    go build -o requesty cmd/requesty/main.go
fi

# Generate the logs
echo "🚀 Generating test logs to $LFO_FUNCTION_APP"
echo "   Duration: $DURATION"
echo "   RPS: $RPS"
echo "   Variety: $VARIETY"
echo ""

cd "$REQUESTY_PATH"

if [ "$VARIETY" = "true" ]; then
    ./requesty \
        -url "https://${LFO_FUNCTION_APP}.azurewebsites.net/api/CustomLog" \
        -key "$LFO_FUNCTION_KEY" \
        -duration "$DURATION" \
        -rps "$RPS" \
        -variety
else
    # Build command as array to avoid eval injection
    CMD=("./requesty"
        -url "https://${LFO_FUNCTION_APP}.azurewebsites.net/api/CustomLog"
        -key "$LFO_FUNCTION_KEY"
        -duration "$DURATION"
        -rps "$RPS"
    )

    if [ -n "$MESSAGE" ]; then
        CMD+=(-message "$MESSAGE")
    else
        CMD+=(-message "Test log from requesty")
    fi

    if [ -n "$LEVEL" ]; then
        CMD+=(-level "$LEVEL")
    else
        CMD+=(-level "info")
    fi

    if [ -n "$COUNT" ]; then
        CMD+=(-count "$COUNT")
    else
        CMD+=(-count "1")
    fi

    "${CMD[@]}"
fi

# Wait a bit for logs to be written to storage
echo ""
echo "⏳ Waiting 10 seconds for logs to be written to storage..."
sleep 10

# Trigger forwarder to process the logs
if [ -n "$LFO_VM_IP" ]; then
    echo "🔄 Triggering forwarder to process logs..."
    ssh -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} "sudo systemctl start datadog-forwarder.service"

    # Check results
    sleep 3
    echo ""
    echo "📊 Checking forwarder results:"
    ssh -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} "sudo journalctl -u datadog-forwarder -n 5 --no-pager | grep 'Finished processing'"
else
    echo "⚠️  Skipping forwarder trigger (VM IP not found)"
fi
