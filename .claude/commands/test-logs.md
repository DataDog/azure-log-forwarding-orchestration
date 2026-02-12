---
name: test-logs
description: Generate test logs to Azure Function App
argument-hint: [--duration=30s] [--rps=10] [--variety]
---

# Generate Test Logs

Generate test logs to Azure Function App using Requesty load tester.

## Usage
Use this command to generate test logs that will be processed by the forwarder. You can customize the duration, RPS, and variety mode.

## Implementation

```bash
#!/bin/bash

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
        --help)
            echo "Usage: /test-logs [--duration=30s] [--rps=10] [--variety] [--message=MSG] [--level=LEVEL] [--count=N]"
            echo ""
            echo "Options:"
            echo "  --duration=TIME  How long to generate logs (default: 30s)"
            echo "  --rps=N          Requests per second (default: 10)"
            echo "  --variety        Use variety mode for fun messages"
            echo "  --message=MSG    Custom log message (when not using variety)"
            echo "  --level=LEVEL    Log level (info/warning/error, default: info)"
            echo "  --count=N        Number of log entries per request (default: 1)"
            echo ""
            echo "Examples:"
            echo "  /test-logs --duration=1m --rps=50"
            echo "  /test-logs --variety"
            echo "  /test-logs --message=\"Production test\" --level=error --count=5"
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

# Validate we have the required resources
if [ -z "$LFO_FUNCTION_APP" ]; then
    echo "❌ Function app not found. Please deploy environment first."
    echo "   Run '/deploy' to create your environment"
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
REQUESTY_PATH="/Users/matt.spurlin/go/src/github.com/DataDog/azure-log-forwarding-orchestration/requesty"

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
    # Build command with optional parameters
    CMD="./requesty -url \"https://${LFO_FUNCTION_APP}.azurewebsites.net/api/CustomLog\" -key \"$LFO_FUNCTION_KEY\" -duration \"$DURATION\" -rps \"$RPS\""

    if [ -n "$MESSAGE" ]; then
        CMD="$CMD -message \"$MESSAGE\""
    else
        CMD="$CMD -message \"Test log from requesty\""
    fi

    if [ -n "$LEVEL" ]; then
        CMD="$CMD -level \"$LEVEL\""
    else
        CMD="$CMD -level \"info\""
    fi

    if [ -n "$COUNT" ]; then
        CMD="$CMD -count \"$COUNT\""
    else
        CMD="$CMD -count \"1\""
    fi

    eval $CMD
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
```

## Examples

```bash
# Generate logs for 1 minute with high RPS
/test-logs --duration=1m --rps=50

# Generate logs with variety mode
/test-logs --variety

# Generate error logs
/test-logs --message="Production test" --level=error --count=5

# Quick test
/test-logs --duration=10s --rps=5
```

## Notes
- The function app needs to be running and accessible
- Logs are written to Azure Storage and picked up by the forwarder
- The forwarder runs every minute via systemd timer, or can be triggered manually
- Use --variety for fun, randomized log messages
