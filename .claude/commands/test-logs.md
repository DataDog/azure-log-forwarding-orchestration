---
name: test-logs
description: Generate test logs to Azure Function App
argument-hint: [--duration=30s] [--rps=10] [--variety]
---

# Generate Test Logs

Generate test logs to Azure Function App using Requesty load tester.

## Usage
Use this command to generate test logs that will be processed by the forwarder. You can customize the duration, RPS, and variety mode.

## Implementation

```bash
#!/bin/bash
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || (cd "$(dirname "$0")/../.." && pwd))"
exec "${REPO_ROOT}/scripts/vm/test-logs.sh" "$@"
```

## Examples

```bash
# Generate logs for 1 minute with high RPS
/test-logs --duration=1m --rps=50

# Generate logs with variety mode
/test-logs --variety

# Generate error logs
/test-logs --message="Production test" --level=error --count=5

# Quick test
/test-logs --duration=10s --rps=5
```

## Notes
- The function app needs to be running and accessible
- Logs are written to Azure Storage and picked up by the forwarder
- The forwarder runs every minute via systemd timer, or can be triggered manually
- Use --variety for fun, randomized log messages
- Standalone script: `scripts/vm/test-logs.sh`
