#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

usage() {
    echo "Usage: $0 [--base-name=<name>] [--skip-agent]"
    echo "       /deploy [--base-name=<name>] [--skip-agent]"
    echo ""
    echo "Deploy a personal forwarder VM environment."
    echo ""
    echo "Options:"
    echo "  --base-name=NAME   Override default base name (default: lfo<username>vm)"
    echo "  --skip-agent       Skip Datadog Agent installation (agent is installed by default)"
    echo "  --help             Show this help message"
}

# Default values
BASE_NAME=""
SKIP_AGENT=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --base-name=*)
            BASE_NAME="${1#*=}"
            shift
            ;;
        --skip-agent)
            SKIP_AGENT="--skip-agent"
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Set base name if provided
if [ -n "$BASE_NAME" ]; then
    export LFO_VM_BASE_NAME="$BASE_NAME"
fi

echo "🚀 Deploying Personal Forwarder VM"
echo "==========================================="
echo "User: ${USER}"
if [ -n "$BASE_NAME" ]; then
    echo "Base Name: $BASE_NAME"
fi
echo ""

# Check prerequisites
if [ -z "$DD_API_KEY" ]; then
    echo "❌ DD_API_KEY not found in environment"
    echo "   Add to ~/.profile: export DD_API_KEY=\"your-api-key\""
    exit 1
fi

if [ -z "${DD_SITE:-}" ]; then
    echo "⚠️  DD_SITE not set, defaulting to datadoghq.com"
    export DD_SITE="datadoghq.com"
fi

echo "Configuration:"
echo "  DD_SITE: $DD_SITE"
echo "  DD_API_KEY: [set, ${#DD_API_KEY} chars]"
echo ""

# Setup Python environment if needed
if [ -d "$REPO_ROOT/venv" ]; then
    source "$REPO_ROOT/venv/bin/activate"
elif [ -d "$HOME/dd/azure-log-forwarding-orchestration/venv" ]; then
    source "$HOME/dd/azure-log-forwarding-orchestration/venv/bin/activate"
else
    echo "Setting up Python virtual environment..."
    cd "$REPO_ROOT"
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
fi

echo "📦 Deploying forwarder..."
echo ""

# Required environment variables
export CONFIG_ID="${CONFIG_ID:-forwarder-vm-config}"
export CONTROL_PLANE_ID="${CONTROL_PLANE_ID:?Must set CONTROL_PLANE_ID}"

# Run deployment script
cd "$REPO_ROOT"
python scripts/deploy_personal_forwarder_vm.py $SKIP_AGENT

# Get VM IP for convenience
USERNAME="${USER:-unknown}"
# Remove dots from username for Azure resource naming
CLEAN_USERNAME="${USERNAME//./}"
BASE_NAME="${LFO_VM_BASE_NAME:-lfo${CLEAN_USERNAME}vm}"
RESOURCE_GROUP="${BASE_NAME}rg"

VM_NAME=$(az vm list --resource-group "$RESOURCE_GROUP" --query "[0].name" -o tsv 2>/dev/null)
if [ -n "$VM_NAME" ]; then
    VM_IP=$(az vm list-ip-addresses --resource-group "$RESOURCE_GROUP" --name "$VM_NAME" \
            --query "[0].virtualMachine.network.publicIpAddresses[0].ipAddress" -o tsv)
    echo ""
    echo "✅ VM deployed successfully!"
    echo "   SSH: ssh azureuser@${VM_IP}"
    echo "   Logs: ssh azureuser@${VM_IP} 'sudo journalctl -u datadog-forwarder -f'"
fi

echo ""
echo "🎯 Next Steps:"
echo "1. Run 'scripts/vm/discover.sh' (or /discover) to see your resources"
echo "2. Run 'scripts/vm/test-logs.sh' (or /test-logs) to create test data"
echo "3. Run 'scripts/vm/forwarder-status.sh' (or /forwarder-status) to check processing"
echo "4. Check logs in Datadog: https://app.datadoghq.com/logs"
