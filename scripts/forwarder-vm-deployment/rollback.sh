#!/bin/bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

#
# Rollback script for Datadog Forwarder
# Switches back to a previous version
#
# Usage: rollback.sh [version]
#   If version not specified, shows available versions and prompts
#

set -euo pipefail

BINARY_DIR="/opt/datadog-forwarder/bin"
CURRENT_LINK="/opt/datadog-forwarder/current"

# Get current version
CURRENT_VERSION=$(basename "$(readlink ${CURRENT_LINK})")
echo "Current version: ${CURRENT_VERSION}"
echo ""

# Function to list versions
list_versions() {
    echo "Available versions:"
    ls -1dt "${BINARY_DIR}"/*/ | while read dir; do
        VERSION=$(basename "${dir}")
        if [ "${VERSION}" = "${CURRENT_VERSION}" ]; then
            echo "  * ${VERSION} (current)"
        else
            # Check if binary exists and is executable
            if [ -f "${dir}/forwarder" ] && [ -x "${dir}/forwarder" ]; then
                echo "    ${VERSION}"
            else
                echo "    ${VERSION} (incomplete)"
            fi
        fi
    done
}

# Function to validate version
validate_version() {
    local version="$1"
    local binary_path="${BINARY_DIR}/${version}/forwarder"

    if [ ! -d "${BINARY_DIR}/${version}" ]; then
        echo "ERROR: Version ${version} does not exist"
        return 1
    fi

    if [ ! -f "${binary_path}" ]; then
        echo "ERROR: Binary not found for version ${version}"
        return 1
    fi

    if [ ! -x "${binary_path}" ]; then
        echo "ERROR: Binary is not executable for version ${version}"
        return 1
    fi

    # Test binary
    echo "Testing binary for version ${version}..."
    if ! sudo -u ddforwarder "${binary_path}" --version > /dev/null 2>&1; then
        echo "ERROR: Binary execution test failed for version ${version}"
        return 1
    fi

    return 0
}

# Function to perform rollback
perform_rollback() {
    local target_version="$1"

    echo "Rolling back to version: ${target_version}"

    # Validate target version
    if ! validate_version "${target_version}"; then
        echo "Rollback aborted"
        exit 1
    fi

    # Stop timer
    echo "Stopping forwarder timer..."
    sudo systemctl stop datadog-forwarder.timer
    if systemctl is-active --quiet datadog-forwarder.service; then
        sudo systemctl stop datadog-forwarder.service
    fi

    # Switch symlink
    echo "Switching to version ${target_version}..."
    sudo ln -sfn "${BINARY_DIR}/${target_version}" "${CURRENT_LINK}"

    # Update VERSION_TAG
    echo "Updating VERSION_TAG..."
    sudo sed -i "s/^VERSION_TAG=.*/VERSION_TAG=\"${target_version}\"/" /etc/datadog-forwarder/environment || {
        echo "VERSION_TAG=\"${target_version}\"" | sudo tee -a /etc/datadog-forwarder/environment > /dev/null
    }

    # Reload systemd
    echo "Reloading systemd..."
    sudo systemctl daemon-reload

    # Start timer
    echo "Starting forwarder timer..."
    sudo systemctl start datadog-forwarder.timer

    # Wait for first execution
    echo "Waiting for first execution..."
    sleep 5

    # Check status
    echo ""
    echo "Checking service status..."
    if sudo systemctl is-failed --quiet datadog-forwarder.service; then
        echo "WARNING: Service appears to have failed after rollback"
        echo "Check logs: sudo journalctl -u datadog-forwarder.service -n 50"
    else
        echo "Service is running"
    fi

    # Show recent logs
    echo ""
    echo "Recent logs:"
    sudo journalctl -u datadog-forwarder.service -n 10 --no-pager

    echo ""
    echo "✅ Rollback complete!"
    echo "Rolled back from ${CURRENT_VERSION} to ${target_version}"
}

# Main logic
if [ $# -eq 0 ]; then
    # No version specified, show menu
    list_versions
    echo ""

    # Get list of valid versions (excluding current)
    VALID_VERSIONS=()
    while IFS= read -r dir; do
        VERSION=$(basename "${dir}")
        if [ "${VERSION}" != "${CURRENT_VERSION}" ]; then
            BINARY_PATH="${dir}/forwarder"
            if [ -f "${BINARY_PATH}" ] && [ -x "${BINARY_PATH}" ]; then
                VALID_VERSIONS+=("${VERSION}")
            fi
        fi
    done < <(ls -1dt "${BINARY_DIR}"/*/)

    if [ ${#VALID_VERSIONS[@]} -eq 0 ]; then
        echo "No other versions available for rollback"
        exit 1
    fi

    # Prompt for version
    echo "Enter version to rollback to (or 'cancel' to abort):"
    read -r TARGET_VERSION

    if [ "${TARGET_VERSION}" = "cancel" ] || [ -z "${TARGET_VERSION}" ]; then
        echo "Rollback cancelled"
        exit 0
    fi
elif [ "$1" = "--list" ]; then
    # Just list versions and exit
    list_versions
    exit 0
else
    # Version specified as argument
    TARGET_VERSION="$1"
fi

# Check if trying to rollback to current version
if [ "${TARGET_VERSION}" = "${CURRENT_VERSION}" ]; then
    echo "Already running version ${TARGET_VERSION}"
    exit 0
fi

# Perform the rollback
perform_rollback "${TARGET_VERSION}"
