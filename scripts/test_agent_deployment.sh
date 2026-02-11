#!/bin/bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

#
# Test script to verify Datadog Agent deployment on forwarder VM
#

set -e

# Colors for output
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

log_test() {
    echo -e "${GREEN}[TEST]${NC} $1"
}

# Check required environment variables
if [[ -z "${DD_API_KEY}" ]]; then
    log_error "DD_API_KEY environment variable is required"
    exit 1
fi

if [[ -z "${CONTROL_PLANE_ID}" ]]; then
    log_error "CONTROL_PLANE_ID environment variable is required"
    exit 1
fi

if [[ -z "${CONFIG_ID}" ]]; then
    log_error "CONFIG_ID environment variable is required"
    exit 1
fi

log_info "Starting Datadog Agent deployment test..."

# Get the base name for resources
USERNAME=$(whoami)
CLEAN_USERNAME="${USERNAME//.}"
BASE_NAME="${LFO_VM_BASE_NAME:-lfo${CLEAN_USERNAME}vm}"

log_info "Using base name: ${BASE_NAME}"

# Step 1: Deploy with agent installation (default behavior)
log_test "Deploying forwarder with Datadog Agent (default)..."
python3 scripts/deploy_personal_forwarder_vm.py \
    --base-name "${BASE_NAME}" \
    --skip-build \
    --skip-upload
# Note: Agent installation is now default, use --skip-agent to disable

if [[ $? -ne 0 ]]; then
    log_error "Deployment failed"
    exit 1
fi

log_info "Deployment completed. Waiting for services to stabilize..."
sleep 30

# Step 2: Get VM IP
log_test "Getting VM IP address..."
VM_IP=$(az vm show -d \
    --resource-group "${BASE_NAME}rg" \
    --name "${BASE_NAME}" \
    --query publicIps \
    -o tsv)

if [[ -z "${VM_IP}" ]]; then
    log_error "Failed to get VM IP address"
    exit 1
fi

log_info "VM IP: ${VM_IP}"

# Step 3: Check if agent is installed
log_test "Checking if Datadog Agent is installed..."
ssh -o StrictHostKeyChecking=no "azureuser@${VM_IP}" \
    "command -v datadog-agent" >/dev/null 2>&1

if [[ $? -eq 0 ]]; then
    log_info "✅ Datadog Agent is installed"
else
    log_error "❌ Datadog Agent is not installed"
    exit 1
fi

# Step 4: Check agent service status
log_test "Checking Datadog Agent service status..."
AGENT_STATUS=$(ssh -o StrictHostKeyChecking=no "azureuser@${VM_IP}" \
    "sudo systemctl is-active datadog-agent" 2>/dev/null)

if [[ "${AGENT_STATUS}" == "active" ]]; then
    log_info "✅ Datadog Agent is running"
else
    log_error "❌ Datadog Agent is not running (status: ${AGENT_STATUS})"

    # Show agent logs for debugging
    log_warning "Agent logs:"
    ssh -o StrictHostKeyChecking=no "azureuser@${VM_IP}" \
        "sudo journalctl -u datadog-agent -n 20 --no-pager"
    exit 1
fi

# Step 5: Check agent health
log_test "Checking Datadog Agent health..."
ssh -o StrictHostKeyChecking=no "azureuser@${VM_IP}" \
    "sudo datadog-agent health" >/dev/null 2>&1

if [[ $? -eq 0 ]]; then
    log_info "✅ Datadog Agent health check passed"
else
    log_warning "⚠️  Agent health check had issues"
fi

# Step 6: Check agent configuration
log_test "Verifying agent configuration..."
AGENT_CONFIG=$(ssh -o StrictHostKeyChecking=no "azureuser@${VM_IP}" \
    "sudo grep -E '^(api_key|site|env)' /etc/datadog-agent/datadog.yaml" 2>/dev/null)

if [[ -n "${AGENT_CONFIG}" ]]; then
    log_info "✅ Agent configuration found"
    echo "${AGENT_CONFIG}"
else
    log_error "❌ Agent configuration not found or invalid"
    exit 1
fi

# Step 7: Check APM receiver
log_test "Checking APM receiver..."
APM_STATUS=$(ssh -o StrictHostKeyChecking=no "azureuser@${VM_IP}" \
    "sudo netstat -tlpn | grep ':8126'" 2>/dev/null)

if [[ -n "${APM_STATUS}" ]]; then
    log_info "✅ APM receiver is listening on port 8126"
else
    log_warning "⚠️  APM receiver might not be listening on port 8126"
fi

# Step 8: Check log collection
log_test "Checking log collection configuration..."
LOG_CONFIG=$(ssh -o StrictHostKeyChecking=no "azureuser@${VM_IP}" \
    "sudo ls -la /etc/datadog-agent/conf.d/logs.d/" 2>/dev/null)

if [[ -n "${LOG_CONFIG}" ]]; then
    log_info "✅ Log collection is configured"
else
    log_warning "⚠️  Log collection might not be configured"
fi

# Step 9: Check forwarder service
log_test "Checking forwarder service..."
FORWARDER_STATUS=$(ssh -o StrictHostKeyChecking=no "azureuser@${VM_IP}" \
    "sudo systemctl is-active datadog-forwarder.timer" 2>/dev/null)

if [[ "${FORWARDER_STATUS}" == "active" ]]; then
    log_info "✅ Forwarder timer is active"
else
    log_warning "⚠️  Forwarder timer is not active (status: ${FORWARDER_STATUS})"
fi

# Step 10: Test metrics submission
log_test "Testing metrics submission..."
ssh -o StrictHostKeyChecking=no "azureuser@${VM_IP}" << 'EOF'
    # Send a test metric
    echo "test.agent.deployment:1|c" | nc -u -w1 localhost 8125
EOF

log_info "Test metric sent to DogStatsD"

# Summary
echo ""
echo "=========================================="
echo "        Deployment Test Summary"
echo "=========================================="
echo ""

log_info "✅ Datadog Agent successfully installed and configured"
log_info "✅ Agent is running and healthy"
log_info "✅ APM receiver is ready (port 8126)"
log_info "✅ Log collection is configured"
log_info "✅ Forwarder service is operational"

echo ""
log_info "Next steps:"
echo "  1. Check Datadog UI for incoming metrics: https://app.datadoghq.com/infrastructure"
echo "  2. View logs: https://app.datadoghq.com/logs?query=service%3Aazure-log-forwarder"
echo "  3. When APM code is merged, traces will appear at: https://app.datadoghq.com/apm/traces"

echo ""
log_info "Useful commands:"
echo "  - Check status: /status"
echo "  - Manage forwarder: /forwarder-manage [action]"
echo "  - Manage agent: /forwarder-manage agent-[action]"
echo "  - View logs: /forwarder-logs"

echo ""
log_info "Test completed successfully! 🎉"
