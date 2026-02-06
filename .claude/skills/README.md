# Claude Code Skills for Personal Forwarder Environment

This directory contains Claude Code skills for managing personal Azure Log Forwarder environments. Each skill is designed to work with dynamically discovered resources based on your username.

## Quick Start

1. **First Time Setup**: Deploy your environment
   ```
   skill: deploy-personal-env
   ```

2. **Discover Your Resources**: Find your VM IP and other resources
   ```
   skill: discover-environment
   ```

3. **Generate Test Data**: Create logs for testing
   ```
   skill: generate-test-logs
   ```

4. **Check Status**: Monitor your forwarder
   ```
   skill: forwarder-status
   ```

## Available Skills

### 🔍 discover-environment
Discovers your Azure resources (VM IP, function app, storage) based on your username.
- **Use First**: Always run this before other skills to find your resources
- Exports environment variables for other scripts
- Shows connection details and status

### 🚀 deploy-personal-env
Deploys a complete personal forwarder environment.
- Creates VM, storage, function app, and all networking
- Configures Datadog integration
- Sets up systemd timer for automatic execution
- **Required Env**: `DD_API_KEY`, `DD_SITE`

### 🔄 update-forwarder-binary
Builds and deploys updated forwarder code to your VM.
- Rebuilds binary from current source
- Deploys to your personal VM
- Restarts the service
- Preserves configuration

### 📝 generate-test-logs
Creates test logs using Requesty load tester.
- Generates logs to your function app
- Customizable duration, RPS, and message variety
- Automatically triggers forwarder processing
- Shows processing results

### 📊 forwarder-status
Comprehensive status check of your forwarder.
- Shows timer and service status
- Displays current configuration
- Recent processing statistics
- Error monitoring

### 📋 view-forwarder-logs
Views detailed forwarder logs from your VM.
- Recent execution logs
- Processing statistics
- Error filtering
- Real-time following option

### 🎮 manage-forwarder
Control the forwarder service on your VM.
- **Actions**: start, stop, restart, trigger, logs, config
- Manage systemd timer
- Update environment variables
- Immediate execution trigger

### 🔎 search-datadog-logs
Search for logs in Datadog using API keys.
- Uses DD_API_KEY and DD_APPLICATION_KEY from environment
- Customizable queries and time ranges
- Shows formatted log results
- Provides direct Datadog UI links
- **Required Env**: `DD_API_KEY`, `DD_APPLICATION_KEY`

## Environment Variables

Set these in `~/.profile` for persistence:

```bash
# Required
export DD_API_KEY="your-datadog-api-key"
export DD_SITE="datadoghq.com"  # or your DD site

# Optional - override defaults
export LFO_VM_BASE_NAME="custom-name"  # Default: lfo<username>vm
export CONFIG_ID="forwarder-vm-config"
export CONTROL_PLANE_ID="d0105e57d837"
```

## Common Workflows

### Initial Setup
```
1. skill: deploy-personal-env          # Deploy everything
2. skill: discover-environment         # Find your resources
3. skill: generate-test-logs           # Create test data
4. skill: forwarder-status             # Verify it's working
```

### Development Workflow
```
1. Edit forwarder code
2. skill: update-forwarder-binary      # Deploy changes
3. skill: generate-test-logs           # Test with data
4. skill: view-forwarder-logs          # Check results
```

### Debugging Issues
```
1. skill: forwarder-status             # Check overall health
2. skill: view-forwarder-logs          # See detailed logs
3. skill: manage-forwarder config      # Check configuration
4. skill: manage-forwarder trigger     # Force immediate run
```

## Resource Naming

Resources are named based on your username:
- Base Name: `lfo<username>vm` (e.g., `lfomattvm`)
- Resource Group: `<basename>rg`
- VM: `<basename>`
- Storage: `<basename>storage` (truncated if needed)
- Function App: `<basename>-loggy`

## Notes

- All skills dynamically discover resources based on your username
- VM IPs and resource names are never hardcoded
- Skills work across team members with different environments
- The forwarder runs every minute via systemd timer
- Logs are forwarded to Datadog based on DD_SITE configuration

## Troubleshooting

If skills can't find your resources:
1. Check you're logged into Azure CLI: `az account show`
2. Verify your username: `echo $USER`
3. Set LFO_VM_BASE_NAME if using custom naming
4. Run `discover-environment` to see what exists
5. Deploy if needed: `deploy-personal-env`

## Contributing

When adding new skills:
- Use dynamic resource discovery (never hardcode IPs)
- Include clear documentation in the skill file
- Support environment variable overrides
- Add error handling for missing resources
- Update this README