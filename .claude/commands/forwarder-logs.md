---
name: forwarder-logs
description: View and analyze forwarder logs from VM
argument-hint: [--lines=N] [--follow] [--filter=pattern]
---

# View Forwarder Logs

View and analyze forwarder logs from the Azure VM.

## Usage
Use this command to check forwarder execution logs, debug issues, and monitor processing statistics.

## Implementation

```bash
#!/bin/bash
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || (cd "$(dirname "$0")/../.." && pwd))"
exec "${REPO_ROOT}/scripts/vm/forwarder-logs.sh" "$@"
```

## Examples

```bash
# View last 50 lines
/forwarder-logs --lines=50

# Follow logs in real-time
/forwarder-logs --follow

# Filter for specific pattern
/forwarder-logs --filter=error --lines=100

# Check processing statistics only
/forwarder-logs --filter="Finished processing"
```

## Useful Filters
- `"Finished processing"` - Show processing summaries
- `"error"` - Show errors
- `"warning"` - Show warnings
- `"Start time"` - Show when runs started
- `"Run time"` - Show execution durations

## Notes
- The forwarder runs every minute via systemd timer
- Logs are managed by systemd journal
- Use `--follow` for real-time monitoring during testing
- VM IP is automatically discovered based on your username or LFO_VM_BASE_NAME
- Standalone script: `scripts/vm/forwarder-logs.sh`
