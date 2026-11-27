---
name: update-binary
description: Build and deploy updated forwarder binary to VM
argument-hint: [--no-restart]
---

# Update Forwarder Binary on VM

Build and deploy an updated forwarder binary to your personal Azure VM.

## Usage
This command rebuilds the forwarder binary from the current code and deploys it to your VM.

## Implementation

```bash
#!/bin/bash
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || (cd "$(dirname "$0")/../.." && pwd))"
exec "${REPO_ROOT}/scripts/vm/update-binary.sh" "$@"
```

## Examples

```bash
# Build and deploy with service restart
/update-binary

# Update binary without restarting service
/update-binary --no-restart
```

## Notes
- VM IP is automatically discovered based on your username or LFO_VM_BASE_NAME
- Requires SSH access to the VM
- The forwarder runs on a systemd timer every minute
- Use --no-restart if you want to update the binary without disrupting the service
- Standalone script: `scripts/vm/update-binary.sh`
