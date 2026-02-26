# Claude Commands for Azure Log Forwarding Orchestration

This directory contains Claude commands for managing personal Azure Log Forwarder environments. These commands replace the older skill-based system with a more intuitive slash-command interface.

## Quick Start

1. **Deploy your environment**: `/deploy`
2. **Discover resources**: `/discover`
3. **Generate test logs**: `/test-logs`
4. **Check status**: `/forwarder-status`
5. **View logs**: `/forwarder-logs`

## Available Commands

### Environment Management

#### `/deploy [forwarder|lfo] [--base-name=<name>]`
Deploy a personal forwarder or the full LFO orchestration environment.

**Examples:**
```bash
/deploy                          # Deploy forwarder (default)
/deploy lfo                      # Deploy LFO orchestration environment
/deploy forwarder --base-name=test1  # Deploy forwarder with custom name
```

#### `/cleanup [--force]`
Delete your entire personal forwarder environment (destructive operation!).

**Examples:**
```bash
/cleanup                         # Delete with confirmation prompts
/cleanup --force                 # Delete without confirmation (dangerous!)
```

#### `/discover [--export]`
Discover and display your Azure resources.

**Examples:**
```bash
/discover                        # Show all resources
/discover --export               # Output as environment variables
/discover --export >> ~/.profile # Save environment permanently
```

### Forwarder Operations

#### `/forwarder-status [--errors-only]`
Check comprehensive forwarder status and health.

**Examples:**
```bash
/forwarder-status                # Full status report
/forwarder-status --errors-only  # Only show errors and issues
```

#### `/forwarder-logs [--lines=N] [--follow] [--filter=pattern]`
View and analyze forwarder logs from VM.

**Examples:**
```bash
/forwarder-logs --lines=50                    # View last 50 lines
/forwarder-logs --follow                      # Follow logs in real-time
/forwarder-logs --filter=error --lines=100    # Filter for errors
```

#### `/forwarder-manage <action>`
Control the forwarder service (start/stop/restart/trigger).

**Actions:**
- `start` - Start the forwarder timer
- `stop` - Stop the forwarder timer and service
- `restart` - Restart the forwarder timer
- `trigger` - Trigger an immediate forwarder run
- `status` - Show forwarder status (default)
- `logs` - Show recent forwarder logs
- `config` - Display forwarder configuration
- `update-env` - Instructions for updating environment variables

**Examples:**
```bash
/forwarder-manage status         # Check status
/forwarder-manage trigger        # Run forwarder immediately
/forwarder-manage restart        # Restart the service
```

#### `/update-binary [--no-restart]`
Build and deploy updated forwarder binary to VM.

**Examples:**
```bash
/update-binary                   # Build and deploy with restart
/update-binary --no-restart      # Update without restarting service
```

### Testing & Monitoring

#### `/test-logs [--duration=30s] [--rps=10] [--variety]`
Generate test logs to Azure Function App.

**Options:**
- `--duration=TIME` - How long to generate logs (default: 30s)
- `--rps=N` - Requests per second (default: 10)
- `--variety` - Use variety mode for fun messages
- `--message=MSG` - Custom log message
- `--level=LEVEL` - Log level (info/warning/error)
- `--count=N` - Number of log entries per request

**Examples:**
```bash
/test-logs --duration=1m --rps=50            # High volume test
/test-logs --variety                         # Fun test messages
/test-logs --message="Error test" --level=error --count=5
```

#### `/search-logs [query] [--hours=N]`
Search for logs in Datadog using API keys.

**Examples:**
```bash
/search-logs                                 # Search default logs
/search-logs "service:azure-log-forwarder"   # Custom query
/search-logs "status:error" --hours=24       # Search errors in last 24h
```

## Prerequisites

### Required Environment Variables
```bash
# Datadog API Keys
export DD_API_KEY="your-api-key"
export DD_APP_KEY="your-app-key"  # or DD_APPLICATION_KEY
export DD_SITE="datadoghq.com"    # or your Datadog site

# Optional: Override default naming
export LFO_VM_BASE_NAME="custom-name"
```

### Required Tools
- Azure CLI (logged in)
- SSH access to Azure VMs
- Python 3 with virtual environment
- Go compiler (for building forwarder)
- jq (for JSON processing)

## Migration from Skills

If you were using the older skill-based system, here's the mapping:

| Old Skill Command | New Command |
|-------------------|-------------|
| `skill: cleanup-personal-env` | `/cleanup` |
| `skill: deploy-personal-env` | `/deploy` |
| `skill: discover-environment` | `/discover` |
| `skill: forwarder-status` | `/forwarder-status` |
| `skill: view-forwarder-logs` | `/forwarder-logs` |
| `skill: manage-forwarder` | `/forwarder-manage` |
| `skill: update-forwarder-binary` | `/update-binary` |
| `skill: generate-test-logs` | `/test-logs` |
| `skill: search-datadog-logs` | `/search-logs` |

## Standalone Scripts

All commands are also available as standalone shell scripts in `scripts/vm/` that can be run directly from the terminal without Claude Code:

```bash
# These are equivalent:
/discover --export              # Claude Code slash command
scripts/vm/discover.sh --export # Standalone script

# All scripts support --help
scripts/vm/deploy.sh --help
scripts/vm/forwarder-status.sh --help
```

| Script | Claude Command |
|--------|---------------|
| `scripts/vm/discover.sh` | `/discover` |
| `scripts/vm/forwarder-status.sh` | `/forwarder-status` |
| `scripts/vm/forwarder-logs.sh` | `/forwarder-logs` |
| `scripts/vm/forwarder-manage.sh` | `/forwarder-manage` |
| `scripts/vm/deploy.sh` | `/deploy` |
| `scripts/vm/cleanup.sh` | `/cleanup` |
| `scripts/vm/update-binary.sh` | `/update-binary` |
| `scripts/vm/test-logs.sh` | `/test-logs` |
| `scripts/vm/search-logs.sh` | `/search-logs` |

## Architecture

### Directory Structure
```
scripts/
├── lib/
│   └── azure-discovery.sh   # Shared discovery library (canonical location)
├── vm/                       # Standalone VM management scripts
│   ├── discover.sh
│   ├── forwarder-status.sh
│   ├── forwarder-logs.sh
│   ├── forwarder-manage.sh
│   ├── deploy.sh
│   ├── cleanup.sh
│   ├── update-binary.sh
│   ├── test-logs.sh
│   └── search-logs.sh
├── run_task.sh
└── deploy_personal_env.py

.claude/
├── commands/           # Thin wrappers that delegate to scripts/vm/
│   ├── cleanup.md
│   ├── deploy.md
│   ├── discover.md
│   ├── forwarder-logs.md
│   ├── forwarder-manage.md
│   ├── status.md
│   ├── update-binary.md
│   ├── test-logs.md
│   └── search-logs.md
├── lib/               # Redirect to scripts/lib/
│   └── azure-discovery.sh
└── skills/            # Legacy skills (deprecated)
```

### Resource Naming Convention

Resources are named based on your username:
- Resource Group: `lfo<username>vmrg`
- VM: `lfo<username>vm`
- Function App: `lfo<username>-loggy`
- Storage Account: `lfo<username>storage`

You can override this with `LFO_VM_BASE_NAME` environment variable.

## Troubleshooting

### Command Not Found
If a command is not recognized:
1. Ensure you're in a Claude Code session
2. Check that the `.claude/commands/` directory exists
3. Try `/help` to see available commands

### Authentication Issues
```bash
# Check Azure CLI login
az account show

# Check Datadog API keys
echo $DD_API_KEY
echo $DD_APP_KEY

# Use dd-auth if available
dd-auth -- /search-logs
```

### Resource Discovery Fails
```bash
# Manually set base name
export LFO_VM_BASE_NAME="your-base-name"
/discover

# List all resource groups
az group list --query "[?contains(name, 'lfo')].name" -o tsv
```

### SSH Connection Issues
```bash
# Test SSH directly
ssh azureuser@<vm-ip>

# Check SSH key
ls ~/.ssh/id_rsa*
```

## Support

For issues or questions about these commands:
1. Check the command help: `/command-name --help`
2. Review the skill documentation in `.claude/skills/` for detailed implementation
3. Check Azure portal for resource status
4. View Datadog logs for processing issues

## Contributing

When adding new commands:
1. Create the standalone script in `scripts/vm/` with proper license header, `set -euo pipefail`, and `usage()` function
2. Source the shared library: `source "${REPO_ROOT}/scripts/lib/azure-discovery.sh"`
3. Create a thin wrapper `.md` file in `.claude/commands/` that `exec`s the script
4. Add YAML frontmatter with name, description, and argument-hint
5. Follow the existing naming patterns for consistency
