# Datadog Forwarder VM Deployment

This directory contains scripts for deploying the Datadog Azure Log Forwarder to Ubuntu VMs.

## Overview

The deployment system provides:
- Automatic Azure resource creation (Resource Group, Storage Account, VM)
- Git-based versioning with commit SHA tracking
- Zero-downtime updates with rollback capability
- Systemd timer-based scheduling (runs every minute)
- Secure binary distribution via Azure Storage

## Prerequisites

1. **Local Requirements:**
   - Azure CLI installed and authenticated
   - Go 1.21+ for building the forwarder
   - SSH key pair (`~/.ssh/id_ed25519`, `id_rsa`, or `id_ecdsa`; ed25519 preferred)
   - Git repository (for version tagging)

2. **Environment Variables:**
   ```bash
   export DD_API_KEY="your-datadog-api-key"
   export CONTROL_PLANE_ID="your-control-plane-id"
   export CONFIG_ID="your-config-id"
   export DD_SITE="datadoghq.com"  # Optional, defaults to datadoghq.com
   ```

## Quick Start

1. **Deploy Everything (First Time):**
   ```bash
   cd scripts
   python3 deploy_personal_forwarder_vm.py
   ```

   This will:
   - Create Azure resources if they don't exist
   - Build the forwarder binary
   - Upload to Azure Storage
   - Deploy to VM and start the systemd timer

2. **Update Existing Deployment:**
   ```bash
   # Make your code changes, then:
   python3 deploy_personal_forwarder_vm.py
   ```

   The script detects existing resources and only updates the binary.

## Deployment Options

```bash
# Use custom base name for resources
python3 deploy_personal_forwarder_vm.py --base-name myforwarder

# Skip building (use existing binary)
python3 deploy_personal_forwarder_vm.py --skip-build

# Skip upload to storage
python3 deploy_personal_forwarder_vm.py --skip-upload

# Skip deployment to VM
python3 deploy_personal_forwarder_vm.py --skip-deploy

# Use specific subscription
python3 deploy_personal_forwarder_vm.py --subscription-id <subscription-id>
```

## Version Management

Versions are automatically generated from git:
- Clean repository: `<short-sha>` (e.g., `d2242ab1`)
- Uncommitted changes: `<short-sha>-dirty` (e.g., `d2242ab1-dirty`)

This version tag is:
- Embedded in the binary
- Used as storage path
- Set as VERSION_TAG environment variable
- Visible in Datadog logs

## VM Management

### SSH Access
```bash
# SSH to VM (IP shown after deployment)
ssh azureuser@<vm-ip>

# Check service status
ssh azureuser@<vm-ip> 'sudo systemctl status datadog-forwarder.timer'

# View logs
ssh azureuser@<vm-ip> 'sudo journalctl -u datadog-forwarder -f'
```

### Manual Operations on VM

```bash
# Stop the forwarder
sudo systemctl stop datadog-forwarder.timer

# Start the forwarder
sudo systemctl start datadog-forwarder.timer

# Check timer schedule
sudo systemctl list-timers datadog-forwarder.timer

# Run forwarder once manually
sudo systemctl start datadog-forwarder.service
```

### Update Binary

```bash
# From local machine (preferred)
python3 deploy_personal_forwarder_vm.py

# Or directly on VM
sudo ~/deployment/update.sh "<connection-string>" <new-version>
```

### Rollback

```bash
# Interactive rollback (shows available versions)
ssh azureuser@<vm-ip> 'sudo ~/deployment/rollback.sh'

# Direct rollback to specific version
ssh azureuser@<vm-ip> 'sudo ~/deployment/rollback.sh d2242ab1'

# List available versions
ssh azureuser@<vm-ip> 'sudo ~/deployment/rollback.sh --list'
```

## Configuration

### Environment Variables

Configuration is stored in `/etc/datadog-forwarder/environment` on the VM:
- `AzureWebJobsStorage`: Azure Storage connection string
- `DD_API_KEY`: Datadog API key
- `DD_SITE`: Datadog site
- `CONTROL_PLANE_ID`: Control plane identifier
- `CONFIG_ID`: Configuration identifier
- `VERSION_TAG`: Git commit SHA
- `NUM_GOROUTINES`: Concurrency level (default: 10)
- `DD_TELEMETRY`: Enable telemetry (default: true)
- `DD_APM_ENABLED`: Enable APM (default: false)
- `PII_SCRUBBER_RULES`: PII scrubbing rules (JSON)

### Systemd Configuration

- **Service**: `/etc/systemd/system/datadog-forwarder.service`
- **Timer**: `/etc/systemd/system/datadog-forwarder.timer`
- **Schedule**: Every minute at :00 seconds
- **Timeout**: 45 seconds per execution
- **User**: ddforwarder (non-root)

## Troubleshooting

### Check Deployment Status
```bash
# On VM
sudo systemctl status datadog-forwarder.timer
sudo systemctl status datadog-forwarder.service
sudo journalctl -u datadog-forwarder -n 50
```

### Common Issues

1. **Binary fails to execute:**
   - Check binary architecture: `file /opt/datadog-forwarder/current/forwarder`
   - Verify permissions: `ls -la /opt/datadog-forwarder/current/forwarder`

2. **Timer not triggering:**
   - Check timer status: `sudo systemctl list-timers`
   - Verify timer is enabled: `sudo systemctl is-enabled datadog-forwarder.timer`

3. **Connection issues:**
   - Verify environment variables: `sudo cat /etc/datadog-forwarder/environment`
   - Test Azure Storage access: `az storage container list`

4. **Rollback needed:**
   - Use rollback script: `sudo ~/deployment/rollback.sh`
   - Manual symlink fix: `sudo ln -sfn /opt/datadog-forwarder/bin/<version> /opt/datadog-forwarder/current`

## Architecture

```
Azure Resources:
├── Resource Group
├── Storage Account
│   └── forwarder container
│       └── <version>/
│           ├── forwarder-linux-amd64
│           └── forwarder-linux-amd64.sha256
├── Virtual Network
├── Network Security Group
└── Virtual Machine (Ubuntu 22.04)

VM File Structure:
/opt/datadog-forwarder/
├── bin/
│   ├── d2242ab1/
│   │   └── forwarder
│   └── d2242ab2-dirty/
│       └── forwarder
└── current -> /opt/datadog-forwarder/bin/d2242ab1

/etc/datadog-forwarder/
└── environment

/etc/systemd/system/
├── datadog-forwarder.service
└── datadog-forwarder.timer
```

## Security Notes

- VM uses SSH key authentication (no passwords)
- Forwarder runs as non-root user `ddforwarder`
- Network Security Group restricts inbound to SSH only
- Binary integrity verified via SHA256 checksum
- Environment file has restricted permissions (640)

## Resource Cleanup

To remove all resources:
```bash
# Get resource group name from deployment
az group delete --name <resource-group-name> --yes
```

## Development Workflow

1. Make code changes
2. Commit changes (optional, but affects version tag)
3. Run deployment script
4. Monitor logs to verify changes
5. Rollback if needed

The deployment is designed to be idempotent - running it multiple times is safe.
