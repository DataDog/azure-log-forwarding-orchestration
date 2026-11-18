---
name: deploy
description: Deploy a personal environment for the forwarder or a complete LFO
argument-hint: [forwarder|lfo] [--base-name=<name>] [--skip-agent]
---

# Deploy Personal Environment

Deploy a personal forwarder environment with either VM, storage, and Loggy function app or the full LFO control plane and Loggy.

## Usage
This command deploys your personal Azure environment for testing the log forwarder or LFO. It creates all necessary resources and configures them properly.

## Prerequisites
- Azure CLI logged in
- DD_API_KEY in environment or ~/.profile
- Python venv configured

## Implementation

```bash
#!/bin/bash
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || (cd "$(dirname "$0")/../.." && pwd))"
# Route LFO deployments to the existing Python script
if [[ "${1:-}" == "lfo" ]]; then
    shift
    # activate venv
    source "$REPO_ROOT/venv/bin/activate" 2>/dev/null || \
        source "$HOME/dd/azure-log-forwarding-orchestration/venv/bin/activate"
    exec python "$REPO_ROOT/scripts/deploy_personal_env.py" "$@"
fi
# Strip optional "forwarder" keyword — deploy.sh is VM-only now
[[ "${1:-}" == "forwarder" ]] && shift
exec "${REPO_ROOT}/scripts/vm/deploy.sh" "$@"
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
- Standalone script: `scripts/vm/deploy.sh`
