#!/bin/bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

# Install and configure Datadog Agent for personal forwarder VM
# This script installs the Datadog Agent and configures it for monitoring the azure-log-forwarder

set -euo pipefail

# Script constants
AGENT_VERSION="${DD_AGENT_VERSION:-7}"  # Use Agent 7 by default
AGENT_CONFIG_DIR="/etc/datadog-agent"
AGENT_LOGS_DIR="/var/log/datadog-agent"
FORWARDER_USER="ddforwarder"

# Color output for visibility
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if running as root
if [[ $EUID -ne 0 ]]; then
    log_error "This script must be run as root"
    exit 1
fi

# Required environment variables
if [[ -z "${DD_API_KEY:-}" ]]; then
    log_error "DD_API_KEY environment variable is required"
    exit 1
fi

if [[ -z "${DD_SITE:-}" ]]; then
    log_error "DD_SITE environment variable is required"
    exit 1
fi

# Optional environment variables with defaults
DD_ENV="${DD_ENV:-personal-dev}"
DD_SERVICE="${DD_SERVICE:-azure-log-forwarder}"
DD_HOSTNAME="${DD_HOSTNAME:-$(hostname)}"
INSTALL_AGENT="${INSTALL_AGENT:-true}"

log_info "Starting Datadog Agent installation..."
log_info "Configuration:"
log_info "  - DD_SITE: ${DD_SITE}"
log_info "  - DD_ENV: ${DD_ENV}"
log_info "  - DD_SERVICE: ${DD_SERVICE}"
log_info "  - DD_HOSTNAME: ${DD_HOSTNAME}"
log_info "  - Agent Version: ${AGENT_VERSION}"

# Install Datadog Agent if requested
if [[ "${INSTALL_AGENT}" == "true" ]]; then
    log_info "Installing Datadog Agent..."

    # Download and run the official install script
    DD_AGENT_MAJOR_VERSION="${AGENT_VERSION}" \
    DD_API_KEY="${DD_API_KEY}" \
    DD_SITE="${DD_SITE}" \
    bash -c "$(curl -L https://s3.amazonaws.com/dd-agent/scripts/install_script.sh)"

    log_info "Datadog Agent installed successfully"
else
    log_info "Skipping agent installation (INSTALL_AGENT=false)"
fi

# Stop the agent while we configure it
log_info "Stopping agent for configuration..."
if systemctl is-active --quiet datadog-agent; then
    systemctl stop datadog-agent
fi

# Create agent configuration directory structure
log_info "Setting up configuration directories..."
mkdir -p "${AGENT_CONFIG_DIR}/conf.d/apm.d"
mkdir -p "${AGENT_CONFIG_DIR}/conf.d/logs.d"
mkdir -p "${AGENT_CONFIG_DIR}/conf.d/process.d"
mkdir -p "${AGENT_CONFIG_DIR}/conf.d/azure_log_forwarder.d"

# Configure main agent settings
log_info "Configuring main agent settings..."
cat > "${AGENT_CONFIG_DIR}/datadog.yaml" <<EOF
# Main Datadog Agent configuration for Azure Log Forwarder VM

# API Configuration
api_key: ${DD_API_KEY}
site: ${DD_SITE}

# Host identification
hostname: ${DD_HOSTNAME}

# Environment tags
tags:
  - env:${DD_ENV}
  - service:${DD_SERVICE}
  - team:${USER:-unknown}
  - deployment:personal-forwarder
  - managed_by:azure-log-forwarding-orchestration

# Logs configuration
logs_enabled: true
logs_config:
  container_collect_all: false
  processing_rules:
    - type: multi_line
      name: forwarder_multiline
      pattern: '^\d{4}-\d{2}-\d{2}'

# APM Configuration (ready for traces when APM code is merged)
apm_config:
  enabled: true
  apm_non_local_traffic: false  # Only accept traces from localhost
  apm_dd_url: https://trace.agent.${DD_SITE}
  env: ${DD_ENV}

# Process Agent Configuration
process_config:
  enabled: true
  process_collection:
    enabled: true
  process_discovery:
    enabled: true

# System probe (for network monitoring)
system_probe_config:
  enabled: false  # Enable if needed for network monitoring

# Compliance/security monitoring
compliance_config:
  enabled: false

# Runtime security
runtime_security_config:
  enabled: false

# Enable health metrics
health_metrics_enabled: true

# Set logging level
log_level: info
log_file: ${AGENT_LOGS_DIR}/agent.log

# Disable unneeded integrations
use_dogstatsd: true
dogstatsd_port: 8125
dogstatsd_non_local_traffic: false

# Performance tuning
check_runners: 4
forwarder_num_workers: 2
forwarder_timeout: 60

# Container monitoring (disabled as we're not using containers)
docker_labels_as_tags: {}
kubernetes_collect_metadata_tags: false
EOF

# Configure APM
log_info "Configuring APM receiver..."
cat > "${AGENT_CONFIG_DIR}/conf.d/apm.d/conf.yaml" <<EOF
# APM Configuration for Azure Log Forwarder
# This prepares the agent to receive traces when APM code is merged

init_config:

instances:
  # The APM receiver is configured through the main datadog.yaml
  # This file ensures the APM check is enabled
EOF

# Configure log collection for forwarder
log_info "Configuring log collection..."
cat > "${AGENT_CONFIG_DIR}/conf.d/logs.d/forwarder.yaml" <<EOF
# Log collection configuration for Azure Log Forwarder
# Forwarder logs to journald; collect via journald source

logs:
  - type: journald
    source: azure-log-forwarder
    service: ${DD_SERVICE}
    include_units:
      - datadog-forwarder.service
    tags:
      - env:${DD_ENV}
      - component:forwarder
EOF

# Configure process monitoring
log_info "Configuring process monitoring..."
cat > "${AGENT_CONFIG_DIR}/conf.d/process.d/conf.yaml" <<EOF
# Process monitoring configuration

init_config:

instances:
  - name: azure-log-forwarder
    search_string:
      - "forwarder"
    exact_match: false
    thresholds:
      critical: [1, 3]  # Alert if less than 1 or more than 3 processes
EOF

# Create custom check for forwarder monitoring (optional)
log_info "Creating custom forwarder check configuration..."
cat > "${AGENT_CONFIG_DIR}/conf.d/azure_log_forwarder.d/conf.yaml" <<EOF
# Custom check configuration for Azure Log Forwarder monitoring
# This file is a placeholder for future custom metrics

init_config:

instances:
  - min_collection_interval: 60
    tags:
      - env:${DD_ENV}
      - service:${DD_SERVICE}
EOF

# Set proper permissions
log_info "Setting configuration permissions..."
chown -R dd-agent:dd-agent "${AGENT_CONFIG_DIR}"
chmod 640 "${AGENT_CONFIG_DIR}/datadog.yaml"
find "${AGENT_CONFIG_DIR}/conf.d/" -type f -exec chmod 644 {} +
find "${AGENT_CONFIG_DIR}/conf.d/" -type d -exec chmod 755 {} +

# Grant dd-agent user read access to forwarder logs
log_info "Configuring log access permissions..."
if getent group ddforwarder > /dev/null 2>&1; then
    usermod -a -G ddforwarder dd-agent
    log_info "Added dd-agent to ddforwarder group for log access"
fi

# Ensure log directory permissions
if [[ -d "/var/log/datadog-forwarder" ]]; then
    chmod 755 /var/log/datadog-forwarder
    find /var/log/datadog-forwarder -name '*.log' -exec chmod 644 {} + 2>/dev/null || true
fi

# Enable and start the agent
log_info "Enabling and starting Datadog Agent..."
systemctl enable datadog-agent
systemctl start datadog-agent

# Wait for agent to start
sleep 5

# Check agent status
log_info "Checking agent status..."
if systemctl is-active --quiet datadog-agent; then
    log_info "Datadog Agent is running"

    # Display agent status
    datadog-agent status || log_warning "Agent status check returned non-zero"

    log_info "Agent installation and configuration completed successfully!"
    log_info "You can check the agent status with: sudo datadog-agent status"
    log_info "Agent logs are available at: ${AGENT_LOGS_DIR}/agent.log"
    log_info "Metrics and logs should appear in Datadog within a few minutes"
else
    log_error "Datadog Agent failed to start"
    log_error "Check logs with: sudo journalctl -u datadog-agent -n 50"
    exit 1
fi

# Create helper script for agent management
log_info "Creating agent helper script..."
cat > /usr/local/bin/dd-agent-helpers <<'HELPER_EOF'
#!/bin/bash
# Helper functions for Datadog Agent management

case "$1" in
    status)
        sudo datadog-agent status
        ;;
    restart)
        sudo systemctl restart datadog-agent
        echo "Agent restarted"
        ;;
    logs)
        sudo journalctl -u datadog-agent -f
        ;;
    config-check)
        sudo datadog-agent configcheck
        ;;
    health)
        sudo datadog-agent health
        ;;
    *)
        echo "Usage: $0 {status|restart|logs|config-check|health}"
        exit 1
        ;;
esac
HELPER_EOF

chmod +x /usr/local/bin/dd-agent-helpers

log_info "Installation complete!"
log_info ""
log_info "Useful commands:"
log_info "  - Agent status:      sudo dd-agent-helpers status"
log_info "  - Restart agent:     sudo dd-agent-helpers restart"
log_info "  - View agent logs:   sudo dd-agent-helpers logs"
log_info "  - Check config:      sudo dd-agent-helpers config-check"
log_info "  - Health check:      sudo dd-agent-helpers health"
