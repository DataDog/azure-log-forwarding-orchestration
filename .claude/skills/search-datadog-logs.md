# Search Datadog Logs

Search for forwarder and loggy logs in Datadog using your API keys.

## Usage
This skill searches Datadog for logs from your personal forwarder environment using the DD_API_KEY and DD_SITE from your environment.

## Parameters
- `QUERY`: Custom search query (optional, defaults to forwarder logs)
- `TIME_RANGE`: Time range in hours (default: 1)

## Implementation

```bash
#!/bin/bash

QUERY="${1:-}"
TIME_RANGE="${2:-1}"

# Get environment configuration
USERNAME="${USER:-unknown}"
BASE_NAME="${LFO_VM_BASE_NAME:-lfo${USERNAME}vm}"

echo "🔍 Searching Datadog Logs"
echo "========================"
echo "User: $USERNAME"
echo "Resources: ${BASE_NAME}*"
echo ""

# Try to get credentials from environment or dd-auth
if [ -z "$DD_API_KEY" ]; then
    # Try to use dd-auth if available
    if command -v dd-auth &> /dev/null; then
        echo "🔐 Using dd-auth for authentication..."
        DD_AUTH_PREFIX="dd-auth --"
        # dd-auth will inject DD_API_KEY and DD_APP_KEY
    else
        echo "❌ DD_API_KEY not found and dd-auth not available"
        echo "   Add to ~/.profile: export DD_API_KEY=\"your-api-key\""
        exit 1
    fi
else
    DD_AUTH_PREFIX=""
fi

if [ -z "$DD_APPLICATION_KEY" ] && [ -z "$DD_APP_KEY" ]; then
    if [ -z "$DD_AUTH_PREFIX" ]; then
        echo "❌ DD_APPLICATION_KEY or DD_APP_KEY not found in environment"
        echo "   Add to ~/.profile: export DD_APP_KEY=\"your-app-key\""
        exit 1
    fi
fi

# Set DD_APPLICATION_KEY from DD_APP_KEY if needed
if [ -n "$DD_APP_KEY" ] && [ -z "$DD_APPLICATION_KEY" ]; then
    export DD_APPLICATION_KEY="$DD_APP_KEY"
fi

DD_SITE="${DD_SITE:-datadoghq.com}"
echo "DD_SITE: $DD_SITE"
echo ""

# Setup Python virtual environment
REPO_ROOT="${REPO_ROOT:-/Users/matt.spurlin/go/src/github.com/DataDog/azure-log-forwarding-orchestration}"
if [ -d "$HOME/dd/azure-log-forwarding-orchestration/venv" ]; then
    VENV_PYTHON="$HOME/dd/azure-log-forwarding-orchestration/venv/bin/python3"
elif [ -d "$REPO_ROOT/venv" ]; then
    VENV_PYTHON="$REPO_ROOT/venv/bin/python3"
else
    echo "⚠️  Python venv not found, using system Python"
    echo "   Run 'deploy-personal-env' skill to set up environment"
    VENV_PYTHON="python3"
fi

# Build query
if [ -n "$QUERY" ]; then
    SEARCH_QUERY="$QUERY"
else
    # Default query for personal forwarder logs
    SEARCH_QUERY="@azure.resource_name:${BASE_NAME}* OR service:azure-log-forwarder"
fi

echo "Query: $SEARCH_QUERY"
echo "Time Range: Last ${TIME_RANGE} hour(s)"
echo ""

# Export variables for Python to use
export SEARCH_QUERY
export TIME_RANGE

# Search using Python and Datadog API
${DD_AUTH_PREFIX} $VENV_PYTHON << 'EOF'
import json
import requests
from datetime import datetime, timedelta
import os

# Get credentials from environment (dd-auth injects these, or they come from shell)
api_key = os.getenv("DD_API_KEY", "")
app_key = os.getenv("DD_APPLICATION_KEY", "") or os.getenv("DD_APP_KEY", "")
dd_site = os.getenv("DD_SITE", "datadoghq.com")

# Get query and time range from environment
search_query = os.getenv("SEARCH_QUERY", "service:azure-log-forwarder")
time_range = int(os.getenv("TIME_RANGE", "1"))

# Calculate time range
now = datetime.utcnow()
start_time = now - timedelta(hours=time_range)

# Build API request
headers = {
    "Content-Type": "application/json",
    "DD-API-KEY": api_key,
    "DD-APPLICATION-KEY": app_key
}

# Datadog logs search API
url = f"https://api.{dd_site}/api/v2/logs/events/search"

data = {
    "filter": {
        "query": search_query,
        "from": start_time.isoformat() + "Z",
        "to": now.isoformat() + "Z"
    },
    "options": {
        "timezone": "UTC"
    },
    "page": {
        "limit": 50
    },
    "sort": "-timestamp"
}

# Make request
response = requests.post(url, headers=headers, json=data)

if response.status_code == 200:
    result = response.json()
    logs = result.get("data", [])

    if logs:
        print(f"✅ Found {len(logs)} logs\\n")
        print("Recent Logs:")
        print("-" * 80)

        for i, log in enumerate(logs[:20], 1):
            attrs = log.get("attributes", {})
            timestamp = attrs.get("timestamp", "N/A")
            service = attrs.get("service", "N/A")
            message = attrs.get("message", "N/A")

            # Truncate long messages
            if len(message) > 150:
                message = message[:147] + "..."

            print(f"[{i}] {timestamp}")
            print(f"    Service: {service}")
            print(f"    Message: {message}")

            # Show relevant attributes
            azure_attrs = attrs.get("attributes", {})
            if azure_attrs:
                resource_name = azure_attrs.get("azure.resource_name", "")
                if resource_name:
                    print(f"    Resource: {resource_name}")

            print()

        if len(logs) > 20:
            print(f"... and {len(logs) - 20} more logs")
    else:
        print("⚠️  No logs found for the specified query and time range")
        print("")
        print("Try:")
        print("  - Increasing the time range")
        print("  - Checking if the forwarder has processed any blobs recently")
        print("  - Generating test logs with 'generate-test-logs' skill")
else:
    print(f"❌ Error searching logs: {response.status_code}")
    error_msg = response.json().get("errors", [response.text[:200]])
    print(f"   {error_msg}")

# Print Datadog UI links
print("")
print("📊 View in Datadog UI:")
base_url = f"https://app.{dd_site}/logs"
encoded_query = requests.utils.quote(search_query)
print(f"   {base_url}?query={encoded_query}")
EOF

echo ""
echo "💡 Tips:"
echo "  - Use custom queries: ./search-datadog-logs.md '@azure.function_name:*loggy*'"
echo "  - Search longer range: ./search-datadog-logs.md '' 24"
echo "  - View all forwarder logs: ./search-datadog-logs.md 'service:azure-log-forwarder'"
```

## Examples

```bash
# Search default forwarder logs (last hour)
./search-datadog-logs.md

# Search with custom query
./search-datadog-logs.md "@azure.function_name:*loggy*"

# Search last 24 hours
./search-datadog-logs.md "" 24

# Search for errors
./search-datadog-logs.md "status:error" 3

# Search by service
./search-datadog-logs.md "service:azure-log-forwarder"

# Search by storage account
./search-datadog-logs.md "@azure.resource_name:*storage*" 2
```

## Notes
- Requires DD_API_KEY and DD_APPLICATION_KEY in environment
- Uses DD_SITE from environment (defaults to datadoghq.com)
- Returns up to 50 logs, shows first 20 in detail
- Automatically builds resource-based queries
- Provides direct links to Datadog UI