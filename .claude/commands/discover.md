---
name: discover
description: Discover and display your Azure resources (LFO or forwarder)
argument-hint: [--export]
---

# Discover Personal Environment

Discover and display your personal environment resources. Supports both LFO (function app) and VM forwarder environments.

## Usage
This command discovers your Azure resources based on your username and environment variables. It tries LFO environments first (3+ function apps), then falls back to VM forwarder environments.

## Implementation

```bash
#!/bin/bash
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || (cd "$(dirname "$0")/../.." && pwd))"
exec "${REPO_ROOT}/scripts/vm/discover.sh" "$@"
```

## Examples

```bash
# Discover and display resources (auto-detects environment type)
/discover

# Output environment variables for export
/discover --export

# Save environment variables permanently
/discover --export >> ~/.profile
```

## Notes
- Auto-detects LFO vs forwarder environments
- LFO environments: resource group `lfo{username}`, 3+ function apps, no VM
- Forwarder environments: resource group with VM, SSH access
- Set LFO_BASE_NAME to override LFO naming, LFO_VM_BASE_NAME for forwarder
- Exports can be saved to ~/.profile for persistence
- Run this before using other forwarder commands
- Standalone script: `scripts/vm/discover.sh`
