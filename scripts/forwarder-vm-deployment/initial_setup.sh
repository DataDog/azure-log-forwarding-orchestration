#!/bin/bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

#
# Initial setup script for Datadog Forwarder on Ubuntu VM
# This script runs once to set up the VM environment
#

set -euo pipefail

echo "Starting Datadog Forwarder initial setup..."

# Create dedicated user for forwarder (if doesn't exist)
if ! id -u ddforwarder >/dev/null 2>&1; then
    echo "Creating ddforwarder user..."
    sudo useradd -r -s /bin/false -m -d /var/lib/ddforwarder ddforwarder
fi

# Create directory structure
echo "Creating directory structure..."
sudo mkdir -p /opt/datadog-forwarder/bin
sudo mkdir -p /etc/datadog-forwarder
sudo mkdir -p /var/log/datadog-forwarder

# Set ownership and permissions
sudo chown -R ddforwarder:ddforwarder /opt/datadog-forwarder
sudo chown -R ddforwarder:ddforwarder /var/log/datadog-forwarder
sudo chown root:ddforwarder /etc/datadog-forwarder
sudo chmod 750 /etc/datadog-forwarder

# Install Azure CLI if not present
if ! command -v az &> /dev/null; then
    echo "Installing Azure CLI..."
    curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
else
    echo "Azure CLI already installed"
fi

# Install Datadog Agent if requested
if [[ "${INSTALL_DD_AGENT:-false}" == "true" ]]; then
    echo "Installing Datadog Agent..."
    if [[ -f ~/deployment/install_datadog_agent.sh ]]; then
        # Pass through environment variables needed for agent configuration
        export DD_API_KEY="${DD_API_KEY}"
        export DD_SITE="${DD_SITE}"
        export DD_ENV="${DD_ENV:-personal-dev}"
        export DD_SERVICE="${DD_SERVICE:-azure-log-forwarder}"
        export DD_HOSTNAME="${DD_HOSTNAME:-$(hostname)}"
        export INSTALL_AGENT="true"

        # Run the agent installation script
        sudo -E bash ~/deployment/install_datadog_agent.sh

        if [[ $? -eq 0 ]]; then
            echo "✅ Datadog Agent installed successfully"
        else
            echo "⚠️  Datadog Agent installation failed, continuing with setup..."
        fi
    else
        echo "⚠️  Datadog Agent installation script not found at ~/deployment/install_datadog_agent.sh"
        echo "    Skipping agent installation..."
    fi
else
    echo "Skipping Datadog Agent installation (INSTALL_DD_AGENT not set to true)"
fi

# Install systemd service file
if [ -f ~/deployment/datadog-forwarder.service ]; then
    echo "Installing systemd service..."
    sudo cp ~/deployment/datadog-forwarder.service /etc/systemd/system/
    sudo chmod 644 /etc/systemd/system/datadog-forwarder.service
fi

# Install systemd timer file
if [ -f ~/deployment/datadog-forwarder.timer ]; then
    echo "Installing systemd timer..."
    sudo cp ~/deployment/datadog-forwarder.timer /etc/systemd/system/
    sudo chmod 644 /etc/systemd/system/datadog-forwarder.timer
fi

# Reload systemd
echo "Reloading systemd daemon..."
sudo systemctl daemon-reload

# Enable timer (but don't start yet - will start after first deployment)
echo "Enabling systemd timer..."
sudo systemctl enable datadog-forwarder.timer

# Set up log rotation
echo "Setting up log rotation..."
cat <<EOF | sudo tee /etc/logrotate.d/datadog-forwarder
/var/log/datadog-forwarder/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0640 ddforwarder ddforwarder
    sharedscripts
    postrotate
        systemctl reload rsyslog 2>/dev/null || true
    endscript
}
EOF

# Create environment file placeholder (will be populated during deployment)
if [ ! -f /etc/datadog-forwarder/environment ]; then
    echo "Creating environment file placeholder..."
    sudo touch /etc/datadog-forwarder/environment
    sudo chown root:root /etc/datadog-forwarder/environment
    sudo chmod 600 /etc/datadog-forwarder/environment
fi

echo "✅ Initial setup complete!"
echo ""
echo "Next steps:"
echo "1. Deploy the forwarder binary using deploy.sh"
echo "2. Configure environment variables in /etc/datadog-forwarder/environment"
echo "3. Start the timer with: sudo systemctl start datadog-forwarder.timer"
