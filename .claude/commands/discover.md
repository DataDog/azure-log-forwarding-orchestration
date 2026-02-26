---
name: discover
description: Discover and display your Azure resources
argument-hint: [--export]
---

# Discover Personal Environment

Discover and display your personal forwarder environment resources.

## Usage
This command discovers your Azure resources based on your username and environment variables. Run this first to find your VM IP, function app name, and other resources.

## Implementation

```bash
#!/bin/bash
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || (cd "$(dirname "$0")/../.." && pwd))"
exec "${REPO_ROOT}/scripts/vm/discover.sh" "$@"
```

## Examples

```bash
# Discover and display resources
/discover

# Output environment variables for export
/discover --export

# Save environment variables permanently
/discover --export >> ~/.profile
```

## Notes
- This discovers resources based on your username
- Set LFO_VM_BASE_NAME to override the default naming
- Exports can be saved to ~/.profile for persistence
- Run this before using other forwarder commands
- Standalone script: `scripts/vm/discover.sh`
