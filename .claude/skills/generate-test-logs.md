# Generate Test Logs

Generate test logs to Azure Function App using Requesty load tester.

## Usage
Use this skill to generate test logs that will be processed by the forwarder. You can customize the duration, RPS, and variety mode.

## Parameters
- `DURATION`: How long to generate logs (default: 30s)
- `RPS`: Requests per second (default: 10)
- `VARIETY`: Use variety mode for fun messages (default: true)

## Implementation

```bash
# Configuration
FUNCTION_APP_NAME="lfoms1829-loggy"
# Function key should be set as environment variable or retrieved dynamically
FUNCTION_KEY="${AZURE_FUNCTION_KEY:-}"
REQUESTY_PATH="/Users/matt.spurlin/go/src/github.com/DataDog/azure-log-forwarding-orchestration/requesty"

# If function key not set, try to get it dynamically
if [ -z "$FUNCTION_KEY" ]; then
    echo "Retrieving function key..."
    RESOURCE_GROUP="rg-lfoms1829"
    KEY_RESPONSE=$(az functionapp function keys list \
        --resource-group "${RESOURCE_GROUP}" \
        --name "${FUNCTION_APP_NAME}" \
        --function-name "CustomLog" 2>/dev/null || echo '{}')
    FUNCTION_KEY=$(echo "$KEY_RESPONSE" | jq -r '.default // empty')

    if [ -z "$FUNCTION_KEY" ]; then
        echo "Warning: Could not retrieve function key. Set AZURE_FUNCTION_KEY environment variable."
    fi
fi

# Parameters with defaults
DURATION="${DURATION:-30s}"
RPS="${RPS:-10}"
VARIETY="${VARIETY:-true}"

# Build requesty if needed
if [ ! -f "$REQUESTY_PATH/requesty" ]; then
    echo "Building requesty..."
    cd "$REQUESTY_PATH"
    go build -o requesty cmd/requesty/main.go
fi

# Generate the logs
echo "🚀 Generating test logs to $FUNCTION_APP_NAME"
echo "   Duration: $DURATION"
echo "   RPS: $RPS"
echo "   Variety: $VARIETY"
echo ""

cd "$REQUESTY_PATH"

if [ "$VARIETY" = "true" ]; then
    ./requesty \
        -url "https://${FUNCTION_APP_NAME}.azurewebsites.net/api/CustomLog" \
        -key "$FUNCTION_KEY" \
        -duration "$DURATION" \
        -rps "$RPS" \
        -variety
else
    ./requesty \
        -url "https://${FUNCTION_APP_NAME}.azurewebsites.net/api/CustomLog" \
        -key "$FUNCTION_KEY" \
        -duration "$DURATION" \
        -rps "$RPS" \
        -message "${MESSAGE:-Test log from requesty}" \
        -level "${LEVEL:-info}" \
        -count "${COUNT:-1}"
fi

# Wait a bit for logs to be written to storage
echo ""
echo "⏳ Waiting 10 seconds for logs to be written to storage..."
sleep 10

# Trigger forwarder to process the logs
echo "🔄 Triggering forwarder to process logs..."
ssh -o StrictHostKeyChecking=no azureuser@20.85.216.189 "sudo systemctl start datadog-forwarder.service"

# Check results
sleep 3
echo ""
echo "📊 Checking forwarder results:"
ssh -o StrictHostKeyChecking=no azureuser@20.85.216.189 "sudo journalctl -u datadog-forwarder -n 5 --no-pager | grep 'Finished processing'"
```

## Examples

```bash
# Generate logs for 1 minute with high RPS
DURATION=60s RPS=50 ./generate-test-logs.md

# Generate logs without variety mode
VARIETY=false MESSAGE="Production test" LEVEL=error COUNT=5 ./generate-test-logs.md

# Quick test
DURATION=10s RPS=5 ./generate-test-logs.md
```

## Notes
- The function app needs to be running and accessible
- Logs are written to Azure Storage and picked up by the forwarder
- The forwarder runs every minute via systemd timer, or can be triggered manually