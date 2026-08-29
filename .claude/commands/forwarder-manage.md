---
name: forwarder-manage
description: Control the forwarder service (start/stop/restart/trigger/status)
argument-hint: <action> [start|stop|restart|trigger|status|config|logs|update-env|agent-*]
---

# Manage Forwarder Service

Control the forwarder service on your personal Azure VM.

## Usage
Manage the forwarder service and timer with start, stop, restart, and trigger operations.

## Implementation

```bash
#!/bin/bash
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || (cd "$(dirname "$0")/../.." && pwd))"
exec "${REPO_ROOT}/scripts/vm/forwarder-manage.sh" "$@"
```

## Examples

```bash
# Forwarder operations
/forwarder-manage status        # Check forwarder status
/forwarder-manage start         # Start the timer
/forwarder-manage stop          # Stop everything
/forwarder-manage trigger       # Trigger immediate run
/forwarder-manage logs          # View forwarder logs
/forwarder-manage config        # Check forwarder configuration
/forwarder-manage update-env    # Update environment variables

# Datadog Agent operations (if installed)
/forwarder-manage agent-status  # Check agent status
/forwarder-manage agent-start   # Start the agent
/forwarder-manage agent-stop    # Stop the agent
/forwarder-manage agent-restart # Restart the agent
/forwarder-manage agent-logs    # View agent logs
/forwarder-manage agent-config  # Check agent configuration
```

## Notes
- The forwarder runs on a systemd timer every minute
- Use 'trigger' for immediate execution during testing
- Configuration changes require timer restart
- The default action is 'status' if none provided
- Datadog Agent commands are only available if agent is installed (use --install-agent flag during deployment)
- Agent provides metrics, logs, process monitoring, and APM receiver for the forwarder
- Standalone script: `scripts/vm/forwarder-manage.sh`
