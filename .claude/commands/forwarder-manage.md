---
name: forwarder-manage
description: Control the forwarder service (start/stop/restart/trigger)
argument-hint: <action> [start|stop|restart|trigger|status|config|logs|update-env]
---

# Manage Forwarder Service

Control the forwarder service on your personal Azure VM.

## Usage
Manage the forwarder service and timer with start, stop, restart, and trigger operations.

## Implementation

```bash
#!/bin/bash

# Get action from arguments
ACTION="${1:-status}"

# Show help if requested
if [ "$ACTION" = "--help" ] || [ "$ACTION" = "-h" ]; then
    echo "Usage: /forwarder-manage <action>"
    echo ""
    echo "Actions:"
    echo "  start       Start the forwarder timer"
    echo "  stop        Stop the forwarder timer and service"
    echo "  restart     Restart the forwarder timer"
    echo "  trigger     Trigger an immediate forwarder run"
    echo "  status      Show forwarder status (default)"
    echo "  logs        Show recent forwarder logs"
    echo "  config      Display forwarder configuration"
    echo "  update-env  Instructions for updating environment variables"
    echo ""
    echo "Examples:"
    echo "  /forwarder-manage status"
    echo "  /forwarder-manage trigger"
    echo "  /forwarder-manage restart"
    exit 0
fi

# Source common discovery functions
CLAUDE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${CLAUDE_DIR}/lib/azure-discovery.sh"

# Discover resources
discover_resources 2>/dev/null
DISCOVERY_RESULT=$?

if [ $DISCOVERY_RESULT -ne 0 ]; then
    echo "❌ Failed to discover resources. Run '/discover' first"
    exit 1
fi

# Check if we have a VM IP
if [ -z "$LFO_VM_IP" ]; then
    echo "❌ No VM IP found. Have you deployed your environment?"
    echo "   Run '/deploy' to create your environment"
    exit 1
fi

echo "🎮 Forwarder Service Manager"
echo "VM: $LFO_VM_IP"
echo "Action: $ACTION"
echo ""

case "$ACTION" in
    start)
        echo "▶️  Starting forwarder timer..."
        ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} << 'EOF'
            sudo systemctl start datadog-forwarder.timer
            sudo systemctl enable datadog-forwarder.timer
            echo "Timer started and enabled"
            sudo systemctl status datadog-forwarder.timer --no-pager | head -5
EOF
        ;;

    stop)
        echo "⏹️  Stopping forwarder..."
        ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} << 'EOF'
            sudo systemctl stop datadog-forwarder.timer
            sudo systemctl stop datadog-forwarder.service
            echo "Timer and service stopped"
EOF
        ;;

    restart)
        echo "🔄 Restarting forwarder timer..."
        ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} << 'EOF'
            sudo systemctl restart datadog-forwarder.timer
            echo "Timer restarted"
            sudo systemctl status datadog-forwarder.timer --no-pager | head -5
EOF
        ;;

    trigger)
        echo "⚡ Triggering immediate forwarder run..."
        ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} << 'EOF'
            sudo systemctl start datadog-forwarder.service
            sleep 2
            echo ""
            echo "Recent execution:"
            sudo journalctl -u datadog-forwarder -n 20 --no-pager | tail -10
EOF
        ;;

    logs)
        echo "📜 Showing recent forwarder logs..."
        ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
            "sudo journalctl -u datadog-forwarder -n 50 --no-pager"
        ;;

    config)
        echo "⚙️  Forwarder configuration:"
        ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} << 'EOF'
            echo "Environment variables:"
            sudo cat /etc/datadog-forwarder/environment
            echo ""
            echo "Timer schedule:"
            sudo systemctl cat datadog-forwarder.timer | grep -E "OnCalendar|OnBootSec"
            echo ""
            echo "Binary version:"
            sudo /usr/local/bin/datadog-forwarder --version 2>/dev/null || echo "Version flag not supported"
EOF
        ;;

    update-env)
        echo "📝 Update environment variables"
        echo "Current configuration:"
        ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} "sudo cat /etc/datadog-forwarder/environment"
        echo ""
        echo "To update, edit the environment file on the VM:"
        echo "  ssh azureuser@${LFO_VM_IP}"
        echo "  sudo vi /etc/datadog-forwarder/environment"
        echo "  sudo systemctl restart datadog-forwarder.timer"
        ;;

    status)
        echo "📊 Forwarder status:"
        ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} << 'EOF'
            echo "Timer status:"
            sudo systemctl status datadog-forwarder.timer --no-pager | head -10
            echo ""
            echo "Last 5 processing runs:"
            sudo journalctl -u datadog-forwarder --no-pager | grep "Finished processing" | tail -5
EOF
        ;;

    *)
        echo "❌ Unknown action: $ACTION"
        echo ""
        echo "Valid actions are: start, stop, restart, trigger, status, logs, config, update-env"
        echo "Use '/forwarder-manage --help' for more information"
        exit 1
        ;;
esac
```

## Examples

```bash
# Check status
/forwarder-manage status

# Start the timer
/forwarder-manage start

# Stop everything
/forwarder-manage stop

# Trigger immediate run
/forwarder-manage trigger

# View logs
/forwarder-manage logs

# Check configuration
/forwarder-manage config

# Update environment variables
/forwarder-manage update-env
```

## Notes
- The forwarder runs on a systemd timer every minute
- Use 'trigger' for immediate execution during testing
- Configuration changes require timer restart
- The default action is 'status' if none provided
