---
name: search-logs
description: Search for logs in Datadog using API keys
argument-hint: [query] [--hours=1]
---

# Search Datadog Logs

Search for forwarder and loggy logs in Datadog using your API keys.

## Usage
This command searches Datadog for logs from your personal forwarder environment using the DD_API_KEY and DD_SITE from your environment.

## Implementation

```bash
#!/bin/bash

# Default values
QUERY=""
TIME_RANGE="1"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --hours=*)
            TIME_RANGE="${1#*=}"
            shift
            ;;
        --help)
            echo "Usage: /search-logs [query] [--hours=N]"
            echo ""
            echo "Arguments:"
            echo "  query         Custom search query (optional)"
            echo ""
            echo "Options:"
            echo "  --hours=N     Time range in hours (default: 1)"
            echo ""
            echo "Examples:"
            echo "  /search-logs"
            echo "  /search-logs \"service:azure-log-forwarder\""
            echo "  /search-logs \"status:error\" --hours=24"
            echo ""
            echo "Requires DD_API_KEY and DD_APPLICATION_KEY (or DD_APP_KEY) in environment."
            exit 0
            ;;
        *)
            # Assume it's the query
            if [ -z "$QUERY" ]; then
                QUERY="$1"
            fi
            shift
            ;;
    esac
done

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
    echo "   Run '/deploy' to set up environment"
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
        print(f"✅ Found {len(logs)} logs\n")
        print("Recent Logs:")
        print("-" * 80)

        for i, log in enumerate(logs[:20], 1):
            attrs = log.get("attributes", {})
            timestamp = attrs.get("timestamp", "N/A")
            message = attrs.get("message", "N/A")
            service = attrs.get("service", "N/A")
            level = attrs.get("status", "N/A")

            # Get Azure resource info if available
            tags = attrs.get("tags", [])
            resource_name = None
            for tag in tags:
                if tag.startswith("azure_resource_name:"):
                    resource_name = tag.split(":", 1)[1]

            print(f"\n{i}. [{timestamp}] [{level}] {service}")
            if resource_name:
                print(f"   Resource: {resource_name}")
            print(f"   Message: {message[:200]}")

    else:
        print("❌ No logs found for the given query and time range")
        print("\nSuggestions:")
        print("  - Try increasing the time range with --hours=24")
        print("  - Check if the forwarder is running: /forwarder-status")
        print("  - Generate test logs: /test-logs")
else:
    print(f"❌ Error searching logs: {response.status_code}")
    print(f"   Response: {response.text}")
    print("\nTroubleshooting:")
    print("  - Verify DD_API_KEY and DD_APP_KEY are correct")
    print("  - Check DD_SITE matches your Datadog instance")

# Show direct link to Datadog
print(f"\n🔗 View in Datadog:")
print(f"   https://app.{dd_site}/logs?query={search_query.replace(' ', '%20')}")
EOF
```

## Examples

```bash
# Search for default forwarder logs
/search-logs

# Search with custom query
/search-logs "service:azure-log-forwarder"
/search-logs "status:error"

# Search last 24 hours
/search-logs --hours=24

# Search for specific resource
/search-logs "@azure.resource_name:lfoms1829*" --hours=2
```

## Notes
- Requires DD_API_KEY and DD_APPLICATION_KEY (or DD_APP_KEY) in environment
- Can use dd-auth tool if available for authentication
- Default query searches for logs from your personal environment
- Shows up to 20 most recent matching logs
- Provides a direct link to view results in Datadog UI
