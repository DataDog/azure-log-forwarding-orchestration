#!/bin/bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

#
# Deploy script for Datadog Forwarder
# Downloads binary from Azure Storage and activates it
#
# Usage: deploy.sh "<connection_string>" <version>
#

set -e

# Check arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 \"<connection_string>\" <version>"
    exit 1
fi

CONNECTION_STRING="$1"
VERSION="$2"
BINARY_DIR="/opt/datadog-forwarder/bin"
BINARY_PATH="${BINARY_DIR}/${VERSION}/forwarder"
CURRENT_LINK="/opt/datadog-forwarder/current"

echo "Deploying Datadog Forwarder version: ${VERSION}"

# Create version directory
echo "Creating version directory..."
sudo mkdir -p "${BINARY_DIR}/${VERSION}"
sudo chown -R ddforwarder:ddforwarder "${BINARY_DIR}"

# Download binary from Azure Storage
echo "Downloading binary from Azure Storage..."
export AZURE_STORAGE_CONNECTION_STRING="${CONNECTION_STRING}"

# Download the binary
az storage blob download \
    --container-name forwarder \
    --name "${VERSION}/forwarder-linux-amd64" \
    --file "/tmp/forwarder-${VERSION}" \
    --no-progress

# Download checksum
az storage blob download \
    --container-name forwarder \
    --name "${VERSION}/forwarder-linux-amd64.sha256" \
    --file "/tmp/forwarder-${VERSION}.sha256" \
    --no-progress

# Verify checksum
echo "Verifying checksum..."
cd /tmp
sha256sum -c "forwarder-${VERSION}.sha256" || {
    echo "ERROR: Checksum verification failed!"
    rm -f "/tmp/forwarder-${VERSION}" "/tmp/forwarder-${VERSION}.sha256"
    exit 1
}

# Move binary to final location
echo "Installing binary..."
sudo mv "/tmp/forwarder-${VERSION}" "${BINARY_PATH}"
sudo chmod 755 "${BINARY_PATH}"
sudo chown ddforwarder:ddforwarder "${BINARY_PATH}"

# Clean up
rm -f "/tmp/forwarder-${VERSION}.sha256"

# Test binary can execute (with required environment variable)
echo "Testing binary..."
sudo -u ddforwarder env AzureWebJobsStorage="${CONNECTION_STRING}" "${BINARY_PATH}" --version || {
    echo "ERROR: Binary execution test failed!"
    exit 1
}

# Stop the timer before switching
echo "Stopping forwarder timer..."
sudo systemctl stop datadog-forwarder.timer || true
sudo systemctl stop datadog-forwarder.service || true

# Update symlink atomically
echo "Updating current version symlink..."
sudo ln -sfn "${BINARY_DIR}/${VERSION}" "${CURRENT_LINK}"

# Update VERSION_TAG in environment file
echo "Updating VERSION_TAG in environment..."
sudo sed -i "s/^VERSION_TAG=.*/VERSION_TAG=\"${VERSION}\"/" /etc/datadog-forwarder/environment || {
    # If VERSION_TAG doesn't exist, append it
    echo "VERSION_TAG=\"${VERSION}\"" | sudo tee -a /etc/datadog-forwarder/environment > /dev/null
}

# Reload systemd to pick up any changes
echo "Reloading systemd daemon..."
sudo systemctl daemon-reload

# Start the timer
echo "Starting forwarder timer..."
sudo systemctl start datadog-forwarder.timer
sudo systemctl enable datadog-forwarder.timer

# Wait for first execution
echo "Waiting for first execution..."
sleep 5

# Check status
echo "Checking service status..."
sudo systemctl status datadog-forwarder.timer --no-pager || true
echo ""
sudo systemctl status datadog-forwarder.service --no-pager || true

# Show recent logs
echo ""
echo "Recent logs:"
sudo journalctl -u datadog-forwarder.service -n 20 --no-pager

# Clean up old versions (keep last 3)
echo ""
echo "Cleaning up old versions..."
cd "${BINARY_DIR}"
ls -1dt */ | tail -n +4 | while read dir; do
    if [ "${dir%/}" != "${VERSION}" ]; then
        echo "Removing old version: ${dir}"
        sudo rm -rf "${dir}"
    fi
done

echo ""
echo "✅ Deployment complete!"
echo "Version ${VERSION} is now active"
echo ""
echo "Useful commands:"
echo "  Check timer status:  sudo systemctl status datadog-forwarder.timer"
echo "  Check service logs:  sudo journalctl -u datadog-forwarder -f"
echo "  List versions:       ls -la ${BINARY_DIR}"
echo "  Rollback:           sudo ~/deployment/rollback.sh"
