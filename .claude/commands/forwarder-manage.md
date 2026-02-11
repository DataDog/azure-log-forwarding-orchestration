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

# Get action from arguments
ACTION="${1:-status}"

# Show help if requested
if [ "$ACTION" = "--help" ] || [ "$ACTION" = "-h" ]; then
    echo "Usage: /forwarder-manage <action>"
    echo ""
    echo "Forwarder Actions:"
    echo "  start         Start the forwarder timer"
    echo "  stop          Stop the forwarder timer and service"
    echo "  restart       Restart the forwarder timer"
    echo "  trigger       Trigger an immediate forwarder run"
    echo "  status        Show forwarder status (default)"
    echo "  logs          Show recent forwarder logs"
    echo "  config        Display forwarder configuration"
    echo "  update-env    Instructions for updating environment variables"
    echo ""
    echo "Datadog Agent Actions:"
    echo "  agent-status  Show Datadog Agent status"
    echo "  agent-start   Start the Datadog Agent"
    echo "  agent-stop    Stop the Datadog Agent"
    echo "  agent-restart Restart the Datadog Agent"
    echo "  agent-logs    Show recent Datadog Agent logs"
    echo "  agent-config  Check Datadog Agent configuration"
    echo ""
    echo "Examples:"
    echo "  /forwarder-manage status"
    echo "  /forwarder-manage trigger"
    echo "  /forwarder-manage restart"
    echo "  /forwarder-manage agent-status"
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

    agent-status)
        echo "🐶 Datadog Agent status:"
        AGENT_INSTALLED=$(ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
            "command -v datadog-agent" 2>/dev/null)

        if [ -z "$AGENT_INSTALLED" ]; then
            echo "❌ Datadog Agent is not installed"
            echo "   To install: Re-deploy (agent is installed by default)"
            echo "   Note: Use --skip-agent flag only if you don't want the agent"
            exit 1
        fi

        ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} << 'EOF'
            echo "Service status:"
            sudo systemctl status datadog-agent --no-pager | head -15
            echo ""
            echo "Agent health:"
            sudo datadog-agent health
            echo ""
            echo "Agent configuration check:"
            sudo datadog-agent configcheck
EOF
        ;;

    agent-start)
        echo "▶️  Starting Datadog Agent..."
        AGENT_INSTALLED=$(ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
            "command -v datadog-agent" 2>/dev/null)

        if [ -z "$AGENT_INSTALLED" ]; then
            echo "❌ Datadog Agent is not installed"
            echo "   To install: Re-deploy (agent is installed by default)"
            echo "   Note: Use --skip-agent flag only if you don't want the agent"
            exit 1
        fi

        ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} << 'EOF'
            sudo systemctl start datadog-agent
            sudo systemctl enable datadog-agent
            echo "Agent started and enabled"
            sleep 3
            sudo systemctl status datadog-agent --no-pager | head -5
EOF
        ;;

    agent-stop)
        echo "⏹️  Stopping Datadog Agent..."
        AGENT_INSTALLED=$(ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
            "command -v datadog-agent" 2>/dev/null)

        if [ -z "$AGENT_INSTALLED" ]; then
            echo "❌ Datadog Agent is not installed"
            exit 1
        fi

        ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} << 'EOF'
            sudo systemctl stop datadog-agent
            echo "Agent stopped"
EOF
        ;;

    agent-restart)
        echo "🔄 Restarting Datadog Agent..."
        AGENT_INSTALLED=$(ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
            "command -v datadog-agent" 2>/dev/null)

        if [ -z "$AGENT_INSTALLED" ]; then
            echo "❌ Datadog Agent is not installed"
            echo "   To install: Re-deploy (agent is installed by default)"
            echo "   Note: Use --skip-agent flag only if you don't want the agent"
            exit 1
        fi

        ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} << 'EOF'
            sudo systemctl restart datadog-agent
            echo "Agent restarted"
            sleep 3
            sudo systemctl status datadog-agent --no-pager | head -5
EOF
        ;;

    agent-logs)
        echo "📜 Showing recent Datadog Agent logs..."
        AGENT_INSTALLED=$(ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
            "command -v datadog-agent" 2>/dev/null)

        if [ -z "$AGENT_INSTALLED" ]; then
            echo "❌ Datadog Agent is not installed"
            exit 1
        fi

        ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
            "sudo journalctl -u datadog-agent -n 50 --no-pager"
        ;;

    agent-config)
        echo "⚙️  Datadog Agent configuration:"
        AGENT_INSTALLED=$(ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} \
            "command -v datadog-agent" 2>/dev/null)

        if [ -z "$AGENT_INSTALLED" ]; then
            echo "❌ Datadog Agent is not installed"
            echo "   To install: Re-deploy (agent is installed by default)"
            echo "   Note: Use --skip-agent flag only if you don't want the agent"
            exit 1
        fi

        ssh -o StrictHostKeyChecking=no azureuser@${LFO_VM_IP} << 'EOF'
            echo "Main configuration:"
            sudo grep -E "^(api_key|site|hostname|env|tags)" /etc/datadog-agent/datadog.yaml | head -20
            echo ""
            echo "APM configuration:"
            sudo grep -E "apm_config:" -A 5 /etc/datadog-agent/datadog.yaml
            echo ""
            echo "Logs configuration:"
            sudo grep -E "logs_enabled:" /etc/datadog-agent/datadog.yaml
            if [ -f /etc/datadog-agent/conf.d/logs.d/forwarder.yaml ]; then
                echo ""
                echo "Forwarder log collection:"
                sudo cat /etc/datadog-agent/conf.d/logs.d/forwarder.yaml
            fi
EOF
        ;;

    *)
        echo "❌ Unknown action: $ACTION"
        echo ""
        echo "Valid actions are:"
        echo "  Forwarder: start, stop, restart, trigger, status, logs, config, update-env"
        echo "  Agent: agent-status, agent-start, agent-stop, agent-restart, agent-logs, agent-config"
        echo "Use '/forwarder-manage --help' for more information"
        exit 1
        ;;
esac
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
