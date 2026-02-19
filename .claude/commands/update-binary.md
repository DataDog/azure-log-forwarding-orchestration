---
name: update-binary
description: Build and deploy updated forwarder binary to VM
argument-hint: [--no-restart]
---

# Update Forwarder Binary on VM

Build and deploy an updated forwarder binary to your personal Azure VM.

## Usage
This command rebuilds the forwarder binary from the current code and deploys it to your VM.

## Implementation

```bash
#!/bin/bash

# Parse arguments
NO_RESTART=false
for arg in "$@"; do
    case $arg in
        --no-restart)
            NO_RESTART=true
            shift
            ;;
        --help)
            echo "Usage: /update-binary [--no-restart]"
            echo ""
            echo "Options:"
            echo "  --no-restart    Don't restart the service after updating"
            echo ""
            echo "This command builds the forwarder binary for Linux and deploys it to your VM."
            exit 0
            ;;
        *)
            ;;
    esac
done

# Source common discovery functions
CLAUDE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${CLAUDE_DIR}/lib/azure-discovery.sh"

# Discover resources
echo "🔍 Discovering Azure resources..."
discover_resources 2>/dev/null
DISCOVERY_RESULT=$?

if [ $DISCOVERY_RESULT -ne 0 ]; then
    echo "❌ Failed to discover resources. Please run '/discover' first."
    exit 1
fi

# Validate we have the VM IP
if [ -z "$LFO_VM_IP" ]; then
    echo "❌ VM IP not found. Please ensure VM is deployed and running."
    echo "   Run '/deploy' to create your environment"
    exit 1
fi

echo "🎯 Target VM: $LFO_VM_IP"

# Set forwarder directory
FORWARDER_DIR="${FORWARDER_DIR:-$(pwd)/forwarder}"
if [ ! -d "$FORWARDER_DIR" ]; then
    FORWARDER_DIR="/Users/matt.spurlin/go/src/github.com/DataDog/azure-log-forwarding-orchestration/forwarder"
fi

# Build the forwarder binary for Linux
echo "Building forwarder binary for Linux..."
cd "$FORWARDER_DIR"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o forwarder-linux-amd64 cmd/forwarder/forwarder.go

if [ $? -ne 0 ]; then
    echo "❌ Build failed!"
    exit 1
fi

# Copy to VM
echo "Copying binary to VM..."
scp -o StrictHostKeyChecking=accept-new forwarder-linux-amd64 azureuser@${LFO_VM_IP}:~/forwarder-updated

# Deploy on VM
echo "Deploying binary on VM..."
if [ "$NO_RESTART" = "true" ]; then
    ssh -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} << 'EOF'
        sudo systemctl stop datadog-forwarder.service
        sudo mv ~/forwarder-updated /usr/local/bin/datadog-forwarder
        sudo chmod +x /usr/local/bin/datadog-forwarder
        sudo chown root:root /usr/local/bin/datadog-forwarder
        echo "✅ Forwarder binary updated (service not restarted)"
EOF
else
    ssh -o StrictHostKeyChecking=accept-new azureuser@${LFO_VM_IP} << 'EOF'
        sudo systemctl stop datadog-forwarder.timer
        sudo systemctl stop datadog-forwarder.service
        sudo mv ~/forwarder-updated /usr/local/bin/datadog-forwarder
        sudo chmod +x /usr/local/bin/datadog-forwarder
        sudo chown root:root /usr/local/bin/datadog-forwarder
        sudo systemctl start datadog-forwarder.timer
        echo "✅ Forwarder binary updated and service restarted!"
        sudo systemctl status datadog-forwarder.timer --no-pager -n 0
EOF
fi
```

## Examples

```bash
# Build and deploy with service restart
/update-binary

# Update binary without restarting service
/update-binary --no-restart
```

## Notes
- VM IP is automatically discovered based on your username or LFO_VM_BASE_NAME
- Requires SSH access to the VM
- The forwarder runs on a systemd timer every minute
- Use --no-restart if you want to update the binary without disrupting the service
