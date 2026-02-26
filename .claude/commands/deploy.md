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
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || (cd "$(dirname "$0")/../.." && pwd))"
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
