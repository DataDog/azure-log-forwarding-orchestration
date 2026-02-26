---
name: cleanup
description: Delete your entire personal forwarder environment (destructive!)
argument-hint: [--force]
---

# Cleanup Personal Environment

Delete your personal forwarder environment and all associated Azure resources.

## Usage
This command deletes your entire personal forwarder environment by removing the resource group.
**WARNING**: This is a destructive operation that cannot be undone!

## Safety Features
- Shows all resources that will be deleted
- Requires explicit confirmation (unless --force is used)
- Validates resource group before deletion

## Implementation

```bash
#!/bin/bash
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || (cd "$(dirname "$0")/../.." && pwd))"
exec "${REPO_ROOT}/scripts/vm/cleanup.sh" "$@"
```

## Examples

```bash
# Delete your personal environment with confirmation
/cleanup

# Delete without confirmation prompts (dangerous!)
/cleanup --force

# Delete a specific environment
LFO_VM_BASE_NAME="lfoms1829" /cleanup
```

## Notes
- Deleting a resource group removes ALL resources within it
- This operation cannot be undone
- Azure may take several minutes to complete the deletion
- No IAM role assignments outside the resource group are affected
- The deletion happens asynchronously but the script monitors progress
- Standalone script: `scripts/vm/cleanup.sh`

## Safety Considerations
- Requires two confirmations before deletion (unless --force is used)
- Shows all resources that will be deleted
- User must type the exact resource group name to confirm
- Validates resource group exists before attempting deletion
