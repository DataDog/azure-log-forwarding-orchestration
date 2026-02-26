---
name: forwarder-status
description: Check comprehensive forwarder status and health
argument-hint: [--errors-only]
---

# Check Forwarder Status

Check the status and health of your personal forwarder deployment.

## Usage
This command provides a comprehensive view of your forwarder's status, including service state, recent logs, and processing statistics.

## Implementation

```bash
#!/bin/bash
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || (cd "$(dirname "$0")/../.." && pwd))"
exec "${REPO_ROOT}/scripts/vm/forwarder-status.sh" "$@"
```

## Examples

```bash
# Full status report
/forwarder-status

# Only show errors and issues
/forwarder-status --errors-only
```

## Notes
- Requires SSH access to the VM
- Shows both timer and service status
- Displays environment configuration
- Shows recent processing statistics and errors
- Use --errors-only for a quick health check
- Standalone script: `scripts/vm/forwarder-status.sh`
