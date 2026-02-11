#!/bin/bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

#
# Zero-downtime update script for Datadog Forwarder
# Downloads new version, validates, and performs atomic switch
#
# Usage: update.sh "<connection_string>" <new_version>
#

set -e

# Check arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 \"<connection_string>\" <new_version>"
    exit 1
fi

CONNECTION_STRING="$1"
NEW_VERSION="$2"
BINARY_DIR="/opt/datadog-forwarder/bin"
NEW_BINARY_PATH="${BINARY_DIR}/${NEW_VERSION}/forwarder"
CURRENT_LINK="/opt/datadog-forwarder/current"
ROLLBACK_MARKER="/tmp/forwarder-update-in-progress"

# Function to rollback on failure
rollback() {
    echo "ERROR: Update failed, initiating rollback..."

    if [ -f "${ROLLBACK_MARKER}" ]; then
        OLD_VERSION=$(cat "${ROLLBACK_MARKER}")
        echo "Rolling back to version: ${OLD_VERSION}"

        # Restore symlink
        sudo ln -sfn "${BINARY_DIR}/${OLD_VERSION}" "${CURRENT_LINK}"

        # Restore VERSION_TAG
        sudo sed -i "s/^VERSION_TAG=.*/VERSION_TAG=\"${OLD_VERSION}\"/" /etc/datadog-forwarder/environment

        # Start timer again
        sudo systemctl start datadog-forwarder.timer

        # Clean up failed version
        sudo rm -rf "${BINARY_DIR}/${NEW_VERSION}"

        rm -f "${ROLLBACK_MARKER}"
        echo "Rollback complete"
    fi

    exit 1
}

# Set up error trap
trap rollback ERR

echo "Starting zero-downtime update to version: ${NEW_VERSION}"

# Get current version for rollback
CURRENT_VERSION=$(basename "$(readlink ${CURRENT_LINK})")
echo "Current version: ${CURRENT_VERSION}"

# Check if new version is same as current
if [ "${CURRENT_VERSION}" = "${NEW_VERSION}" ]; then
    echo "Version ${NEW_VERSION} is already active"
    exit 0
fi

# Save current version for rollback
echo "${CURRENT_VERSION}" > "${ROLLBACK_MARKER}"

# Check if new version already exists
if [ -d "${BINARY_DIR}/${NEW_VERSION}" ]; then
    echo "Version ${NEW_VERSION} already exists, checking binary..."
    if [ -f "${NEW_BINARY_PATH}" ]; then
        echo "Binary exists, testing execution..."
        sudo -u ddforwarder env AzureWebJobsStorage="${CONNECTION_STRING}" "${NEW_BINARY_PATH}" --version || {
            echo "ERROR: Existing binary failed execution test"
            rm -f "${ROLLBACK_MARKER}"
            exit 1
        }
        echo "Existing binary is valid, proceeding with switch..."
    else
        echo "Binary missing, will re-download..."
        sudo rm -rf "${BINARY_DIR}/${NEW_VERSION}"
    fi
fi

# Download new version if needed
if [ ! -f "${NEW_BINARY_PATH}" ]; then
    echo "Downloading new version..."

    # Create version directory
    sudo mkdir -p "${BINARY_DIR}/${NEW_VERSION}"
    sudo chown -R ddforwarder:ddforwarder "${BINARY_DIR}"

    # Download binary
    export AZURE_STORAGE_CONNECTION_STRING="${CONNECTION_STRING}"

    az storage blob download \
        --container-name forwarder \
        --name "${NEW_VERSION}/forwarder-linux-amd64" \
        --file "/tmp/forwarder-${NEW_VERSION}" \
        --no-progress

    # Download checksum
    az storage blob download \
        --container-name forwarder \
        --name "${NEW_VERSION}/forwarder-linux-amd64.sha256" \
        --file "/tmp/forwarder-${NEW_VERSION}.sha256" \
        --no-progress

    # Verify checksum
    echo "Verifying checksum..."
    cd /tmp
    sha256sum -c "forwarder-${NEW_VERSION}.sha256" || {
        echo "ERROR: Checksum verification failed!"
        rm -f "/tmp/forwarder-${NEW_VERSION}" "/tmp/forwarder-${NEW_VERSION}.sha256"
        rollback
    }

    # Move binary to final location
    sudo mv "/tmp/forwarder-${NEW_VERSION}" "${NEW_BINARY_PATH}"
    sudo chmod 755 "${NEW_BINARY_PATH}"
    sudo chown ddforwarder:ddforwarder "${NEW_BINARY_PATH}"

    # Clean up
    rm -f "/tmp/forwarder-${NEW_VERSION}.sha256"
fi

# Test new binary
echo "Testing new binary..."
sudo -u ddforwarder env AzureWebJobsStorage="${CONNECTION_STRING}" "${NEW_BINARY_PATH}" --version || rollback

# Wait for current execution to complete (max 45 seconds)
echo "Waiting for current execution to complete..."
MAX_WAIT=50
WAITED=0
while sudo systemctl is-active --quiet datadog-forwarder.service && [ $WAITED -lt $MAX_WAIT ]; do
    echo -n "."
    sleep 1
    WAITED=$((WAITED + 1))
done
echo ""

# Stop timer (but not forcefully kill service)
echo "Pausing timer..."
sudo systemctl stop datadog-forwarder.timer

# Perform atomic switch
echo "Switching to new version..."
sudo ln -sfn "${BINARY_DIR}/${NEW_VERSION}" "${CURRENT_LINK}"

# Update VERSION_TAG
sudo sed -i "s/^VERSION_TAG=.*/VERSION_TAG=\"${NEW_VERSION}\"/" /etc/datadog-forwarder/environment

# Reload systemd
sudo systemctl daemon-reload

# Start timer again
echo "Resuming timer..."
sudo systemctl start datadog-forwarder.timer

# Wait for first execution with new version
echo "Waiting for first execution with new version..."
sleep 5

# Health check - verify service started successfully
echo "Performing health check..."
if sudo systemctl is-failed --quiet datadog-forwarder.service; then
    echo "ERROR: Service failed with new version"
    rollback
fi

# Check if binary is actually running
RUNNING_PID=$(sudo systemctl show -p MainPID --value datadog-forwarder.service)
if [ "${RUNNING_PID}" = "0" ]; then
    # Service not currently running, check last execution
    if sudo journalctl -u datadog-forwarder.service -n 10 --no-pager | grep -q "ERROR\|FATAL\|panic"; then
        echo "ERROR: Service logs show errors"
        rollback
    fi
fi

# Success - remove rollback marker
rm -f "${ROLLBACK_MARKER}"

# Show status
echo ""
echo "Update successful!"
sudo systemctl status datadog-forwarder.timer --no-pager || true
echo ""
echo "Recent logs with new version:"
sudo journalctl -u datadog-forwarder.service -n 10 --no-pager | grep "${NEW_VERSION}" || true

# Clean up old versions (keep last 3)
echo ""
echo "Cleaning up old versions..."
cd "${BINARY_DIR}"
ls -1dt */ | tail -n +4 | while read dir; do
    VERSION_NAME="${dir%/}"
    if [ "${VERSION_NAME}" != "${NEW_VERSION}" ] && [ "${VERSION_NAME}" != "${CURRENT_VERSION}" ]; then
        echo "Removing old version: ${VERSION_NAME}"
        sudo rm -rf "${dir}"
    fi
done

echo ""
echo "✅ Zero-downtime update complete!"
echo "Old version: ${CURRENT_VERSION}"
echo "New version: ${NEW_VERSION}"
echo ""
echo "If issues arise, rollback with: sudo ~/deployment/rollback.sh ${CURRENT_VERSION}"
