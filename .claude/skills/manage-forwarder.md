# Manage Forwarder Service

Control the forwarder service on your personal Azure VM.

## Usage
Manage the forwarder service and timer with start, stop, restart, and trigger operations.

## Parameters
- `ACTION`: The action to perform (start|stop|restart|trigger|logs|config)

## Implementation

```bash
#!/bin/bash

ACTION="${1:-status}"

# Discover environment
USERNAME="${USER:-unknown}"
BASE_NAME="${LFO_VM_BASE_NAME:-lfo${USERNAME}vm}"
RESOURCE_GROUP="${BASE_NAME}rg"

# Get VM IP
if [ -z "$LFO_VM_IP" ]; then
    echo "🔍 Discovering VM..."
    VM_NAME=$(az vm list --resource-group "$RESOURCE_GROUP" --query "[0].name" -o tsv 2>/dev/null)
    if [ -z "$VM_NAME" ]; then
        echo "❌ No VM found. Run 'discover-environment' skill first"
        exit 1
    fi
    VM_IP=$(az vm list-ip-addresses --resource-group "$RESOURCE_GROUP" --name "$VM_NAME" \
            --query "[0].virtualMachine.network.publicIpAddresses[0].ipAddress" -o tsv)
else
    VM_IP="$LFO_VM_IP"
fi

echo "🎮 Forwarder Service Manager"
echo "VM: $VM_IP"
echo "Action: $ACTION"
echo ""

case "$ACTION" in
    start)
        echo "▶️  Starting forwarder timer..."
        ssh -o StrictHostKeyChecking=no azureuser@${VM_IP} << 'EOF'
            sudo systemctl start datadog-forwarder.timer
            sudo systemctl enable datadog-forwarder.timer
            echo "Timer started and enabled"
            sudo systemctl status datadog-forwarder.timer --no-pager | head -5
EOF
        ;;

    stop)
        echo "⏹️  Stopping forwarder..."
        ssh -o StrictHostKeyChecking=no azureuser@${VM_IP} << 'EOF'
            sudo systemctl stop datadog-forwarder.timer
            sudo systemctl stop datadog-forwarder.service
            echo "Timer and service stopped"
EOF
        ;;

    restart)
        echo "🔄 Restarting forwarder timer..."
        ssh -o StrictHostKeyChecking=no azureuser@${VM_IP} << 'EOF'
            sudo systemctl restart datadog-forwarder.timer
            echo "Timer restarted"
            sudo systemctl status datadog-forwarder.timer --no-pager | head -5
EOF
        ;;

    trigger)
        echo "⚡ Triggering immediate forwarder run..."
        ssh -o StrictHostKeyChecking=no azureuser@${VM_IP} << 'EOF'
            sudo systemctl start datadog-forwarder.service
            sleep 2
            echo ""
            echo "Recent execution:"
            sudo journalctl -u datadog-forwarder -n 20 --no-pager | tail -10
EOF
        ;;

    logs)
        echo "📜 Showing recent forwarder logs..."
        ssh -o StrictHostKeyChecking=no azureuser@${VM_IP} \
            "sudo journalctl -u datadog-forwarder -n 50 --no-pager"
        ;;

    config)
        echo "⚙️  Forwarder configuration:"
        ssh -o StrictHostKeyChecking=no azureuser@${VM_IP} << 'EOF'
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
        ssh -o StrictHostKeyChecking=no azureuser@${VM_IP} "sudo cat /etc/datadog-forwarder/environment"
        echo ""
        echo "To update, edit the environment file on the VM:"
        echo "  ssh azureuser@${VM_IP}"
        echo "  sudo vi /etc/datadog-forwarder/environment"
        echo "  sudo systemctl restart datadog-forwarder.timer"
        ;;

    status|*)
        echo "📊 Forwarder status:"
        ssh -o StrictHostKeyChecking=no azureuser@${VM_IP} << 'EOF'
            echo "Timer status:"
            sudo systemctl status datadog-forwarder.timer --no-pager | head -10
            echo ""
            echo "Last 5 processing runs:"
            sudo journalctl -u datadog-forwarder --no-pager | grep "Finished processing" | tail -5
EOF
        ;;
esac
```

## Examples

```bash
# Check status
./manage-forwarder.md status

# Start the timer
./manage-forwarder.md start

# Stop everything
./manage-forwarder.md stop

# Trigger immediate run
./manage-forwarder.md trigger

# View logs
./manage-forwarder.md logs

# Check configuration
./manage-forwarder.md config

# Update environment variables
./manage-forwarder.md update-env
```

## Notes
- The forwarder runs on a systemd timer every minute
- Use 'trigger' for immediate execution during testing
- Configuration changes require timer restart