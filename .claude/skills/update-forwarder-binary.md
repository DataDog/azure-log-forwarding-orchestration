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
# Discover environment
USERNAME="${USER:-unknown}"
BASE_NAME="${LFO_VM_BASE_NAME:-lfo${USERNAME}vm}"
RESOURCE_GROUP="${BASE_NAME}rg"

# Try to get VM IP from environment or discover it
if [ -z "$LFO_VM_IP" ]; then
    echo "🔍 Discovering VM IP..."
    VM_NAME=$(az vm list --resource-group "$RESOURCE_GROUP" --query "[0].name" -o tsv 2>/dev/null)
    if [ -z "$VM_NAME" ]; then
        echo "❌ No VM found in resource group $RESOURCE_GROUP"
        echo "   Run 'discover-environment' skill first or deploy your environment"
        exit 1
    fi
    VM_IP=$(az vm list-ip-addresses --resource-group "$RESOURCE_GROUP" --name "$VM_NAME" \
            --query "[0].virtualMachine.network.publicIpAddresses[0].ipAddress" -o tsv)
else
    VM_IP="$LFO_VM_IP"
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
- The VM IP is currently hardcoded to 20.85.216.189
- Requires SSH access to the VM
- The forwarder runs on a systemd timer every minute