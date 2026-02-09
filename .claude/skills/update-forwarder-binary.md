# Update Forwarder Binary on VM

Build and deploy an updated forwarder binary to your personal Azure VM.

## Usage
This skill rebuilds the forwarder binary from the current code and deploys it to your VM.

## Prerequisites
- Run `discover-environment` skill first to find your VM IP
- Or set LFO_VM_IP environment variable

## Steps
1. Discover VM IP dynamically
2. Build the forwarder binary for Linux
3. Copy it to the VM
4. Replace the existing binary
5. Restart the service

## Implementation

```bash
# Source common discovery functions
SCRIPT_DIR="$(dirname "$0")"
source "${SCRIPT_DIR}/common-discovery.sh"

# Discover resources
echo "🔍 Discovering Azure resources..."
if ! discover_resources; then
    echo "❌ Failed to discover resources. Please run 'discover-environment' skill first."
    exit 1
fi

# Configuration from discovered resources
VM_IP="${LFO_VM_IP}"
STORAGE_CONNECTION="${LFO_STORAGE_CONNECTION_STRING}"

# Validate we have the VM IP
if [ -z "$VM_IP" ]; then
    echo "❌ VM IP not found. Please ensure VM is deployed and running."
    exit 1
fi

echo "🎯 Target VM: $VM_IP"

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
scp -o StrictHostKeyChecking=no forwarder-linux-amd64 azureuser@${VM_IP}:~/forwarder-updated

# Deploy on VM
echo "Deploying binary on VM..."
ssh -o StrictHostKeyChecking=no azureuser@${VM_IP} << 'EOF'
    sudo systemctl stop datadog-forwarder.timer
    sudo systemctl stop datadog-forwarder.service
    sudo mv ~/forwarder-updated /usr/local/bin/datadog-forwarder
    sudo chmod +x /usr/local/bin/datadog-forwarder
    sudo chown root:root /usr/local/bin/datadog-forwarder
    sudo systemctl start datadog-forwarder.timer
    echo "✅ Forwarder binary updated successfully!"
    sudo systemctl status datadog-forwarder.timer --no-pager -n 0
EOF
```

## Notes
- VM IP is automatically discovered based on your username or LFO_VM_BASE_NAME
- Requires SSH access to the VM
- The forwarder runs on a systemd timer every minute
- Storage connection is automatically discovered from your resources