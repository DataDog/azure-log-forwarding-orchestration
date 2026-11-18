---
name: status
description: Check comprehensive status and health for personal deployments
argument-hint: [--errors-only]
---

# Check Personal Environment Status

Check the status and health of your personal deployment.

## Usage
This command provides a comprehensive view of your forwarder or LFO deployment's status.

## Implementation

```bash
#!/bin/bash
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || (cd "$(dirname "$0")/../.." && pwd))"
exec "${REPO_ROOT}/scripts/status.sh" "$@"
```

## Examples

```bash
# Full status report
/status

# Only show errors and issues
/status --errors-only
```

## Notes
- Requires SSH access to the VM
- Shows both timer and service status
- Displays environment configuration
- Shows recent processing statistics and errors
- Use --errors-only for a quick health check
- Standalone script: `scripts/status.sh`
