---
name: deploy
description: Deploy a complete personal forwarder environment
argument-hint: [forwarder|lfo] [--base-name=<name>] [--skip-agent]
---

# Deploy Personal Forwarder Environment

Deploy a complete personal forwarder environment with VM, storage, and function app.

## Usage
This command deploys your personal Azure environment for testing the log forwarder. It creates all necessary resources and configures them properly.

## Prerequisites
- Azure CLI logged in
- DD_API_KEY in environment or ~/.profile
- Python venv configured

## Implementation

```bash
#!/bin/bash

# Default values
DEPLOYMENT_TYPE="forwarder"
BASE_NAME=""
SKIP_AGENT=""
REPO_ROOT="${REPO_ROOT:-/Users/matt.spurlin/go/src/github.com/DataDog/azure-log-forwarding-orchestration}"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        forwarder|lfo)
            DEPLOYMENT_TYPE="$1"
            shift
            ;;
        --base-name=*)
            BASE_NAME="${1#*=}"
            shift
            ;;
        --skip-agent)
            SKIP_AGENT="--skip-agent"
            shift
            ;;
        --help)
            echo "Usage: /deploy [forwarder|lfo] [--base-name=<name>] [--skip-agent]"
            echo ""
            echo "Arguments:"
            echo "  forwarder       Deploy the forwarder to a VM (default)"
            echo "  lfo             Deploy the full LFO orchestration environment"
            echo ""
            echo "Options:"
            echo "  --base-name=NAME   Override default base name (default: lfo<username>vm)"
            echo "  --skip-agent       Skip Datadog Agent installation (agent is installed by default)"
            echo ""
            echo "Examples:"
            echo "  /deploy                        # Deploy forwarder (default)"
            echo "  /deploy lfo                    # Deploy LFO environment"
            echo "  /deploy forwarder --skip-agent # Deploy forwarder without agent"
            echo "  /deploy forwarder --base-name=test1  # Deploy forwarder with custom name"
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

echo "🚀 Deploying Personal Forwarder Environment"
echo "==========================================="
echo "User: ${USER}"
echo "Deployment Type: $DEPLOYMENT_TYPE"
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

if [ -z "$DD_SITE" ]; then
    echo "⚠️  DD_SITE not set, defaulting to datadoghq.com"
    export DD_SITE="datadoghq.com"
fi

echo "Configuration:"
echo "  DD_SITE: $DD_SITE"
echo "  DD_API_KEY: ${DD_API_KEY:0:10}..."
echo ""

# Setup Python environment if needed
if [ ! -d "$HOME/dd/azure-log-forwarding-orchestration/venv" ]; then
    echo "Setting up Python virtual environment..."
    cd "$HOME/dd/azure-log-forwarding-orchestration" || cd "$REPO_ROOT"
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
else
    source "$HOME/dd/azure-log-forwarding-orchestration/venv/bin/activate" 2>/dev/null || \
    source "$REPO_ROOT/venv/bin/activate"
fi

# Deploy based on type
case "$DEPLOYMENT_TYPE" in
    forwarder)
        echo "📦 Deploying forwarder..."
        echo ""

        # Required environment variables
        export CONFIG_ID="${CONFIG_ID:-forwarder-vm-config}"
        export CONTROL_PLANE_ID="${CONTROL_PLANE_ID:-d0105e57d837}"

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
        ;;

    lfo)
        echo "📦 Deploying LFO orchestration environment..."
        echo ""
        cd "$REPO_ROOT"
        python scripts/deploy_personal_env.py
        ;;

    *)
        echo "❌ Invalid deployment type: $DEPLOYMENT_TYPE"
        echo "   Use: forwarder or lfo"
        exit 1
        ;;
esac

echo ""
echo "🎯 Next Steps:"
echo "1. Run '/discover' to see your resources"
echo "2. Run '/test-logs' to create test data"
echo "3. Run '/status' to check processing"
echo "4. Check logs in Datadog: https://app.datadoghq.com/logs"
```

## Examples

```bash
# Deploy forwarder with Datadog Agent (recommended)
/deploy
/deploy forwarder

# Deploy forwarder without agent
/deploy forwarder --skip-agent

# Deploy LFO orchestration environment
/deploy lfo

# Use custom base name (with agent)
/deploy forwarder --base-name=mytestenv

# Custom name without agent
/deploy forwarder --base-name=mytestenv --skip-agent
```

## Notes
- VM deployment is recommended for development
- Creates all Azure resources automatically
- Installs Datadog Agent by default for full observability (use --skip-agent to disable)
- Configures Datadog integration with metrics, logs, and process monitoring
- Sets up systemd timer for periodic execution
- Deploys Loggy function app for testing
- Agent APM receiver is ready for traces when APM code is merged
