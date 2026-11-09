#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

usage() {
    echo "Usage: $0 [--no-restart]"
    echo "       /update-binary [--no-restart]"
    echo ""
    echo "Build and deploy updated forwarder binary to VM."
    echo ""
    echo "Options:"
    echo "  --no-restart    Don't restart the service after updating"
    echo "  --help          Show this help message"
}

# Parse arguments
NO_RESTART=false
for arg in "$@"; do
    case $arg in
        --no-restart)
            NO_RESTART=true
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            ;;
    esac
done

# Source common discovery functions
source "${REPO_ROOT}/scripts/vm/lib/azure-discovery.sh"

# Discover resources
echo "🔍 Discovering Azure resources..."
discover_resources 2>/dev/null
DISCOVERY_RESULT=$?

if [ $DISCOVERY_RESULT -ne 0 ]; then
    echo "❌ Failed to discover resources. Please run '/discover' or scripts/vm/discover.sh first."
    exit 1
fi

# Validate we have the VM IP
if [ -z "$LFO_VM_IP" ]; then
    echo "❌ VM IP not found. Please ensure VM is deployed and running."
    echo "   Run '/deploy' or scripts/vm/deploy.sh to create your environment"
    exit 1
fi

echo "🎯 Target VM: $LFO_VM_IP"

# Set forwarder directory
FORWARDER_DIR="${REPO_ROOT}/forwarder"
if [ ! -d "$FORWARDER_DIR" ]; then
    echo "❌ Forwarder directory not found at $FORWARDER_DIR"
    exit 1
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
