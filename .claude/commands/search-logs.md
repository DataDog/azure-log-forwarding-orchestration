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
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || (cd "$(dirname "$0")/../.." && pwd))"
exec "${REPO_ROOT}/scripts/vm/search-logs.sh" "$@"
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
- Standalone script: `scripts/vm/search-logs.sh`
