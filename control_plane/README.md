# Getting Started with the Control Plane

All commands assume you are in the repository root.

## One time setup

Set up Local Dev Environment:
```bash
pyenv install 3.11.8
brew install pyenv-virtualenv
pyenv virtualenv 3.11.8 lfo
pyenv local lfo; pyenv shell lfo
pip install -e './control_plane[dev]'
pre-commit install
```

Make sure you have the Azure CLI installed and are logged in.
```bash
brew install azure-cli
az login
```

Make sure you have Docker installed and the daemon running (required for building container images).
```bash
brew install --cask docker
# Start Docker Desktop or ensure the daemon is running
```

## IDE Setup
For VSCode, install the [reccomended extensions](./control_plane/.vscode/extensions.json) which should be suggested by your IDE when you open the control_plane as the workspace.

For Pycharm, just install the [ruff plugin](https://plugins.jetbrains.com/plugin/20574-ruff).


## Deploying to a personal environment

### Required Permissions

Before deploying, ensure you have the following Azure permissions:

1. **Management Group Contributor** on `Azure-Integrations-Mg` - Required for the ARM template deployment at management group scope. Ask an admin to grant this:
   ```bash
   az role assignment create \
     --assignee "your-email@example.com" \
     --role "Contributor" \
     --scope "/providers/Microsoft.Management/managementGroups/Azure-Integrations-Mg"
   ```

2. **Subscription Contributor** - Required for creating resources in your subscription

### Environment Variables

Set the required environment variables before deploying:
```bash
# Required: Your Datadog API key (find it at https://app.datadoghq.com/organization-settings/api-keys)
export DD_API_KEY=your_datadog_api_key

# Optional: Datadog site (defaults to datadoghq.com)
export DD_SITE=datadoghq.com  # or datadoghq.eu, us3.datadoghq.com, etc.

# Optional: Custom base name for resources (defaults to lfo + your username)
export LFO_BASE_NAME=lfomyname

# Optional: Azure subscription ID (defaults to current az cli subscription)
export AZURE_SUBSCRIPTION_ID=your-subscription-id
```

### Running the Deployment

```bash
# Full deployment (builds docker images, deploys ARM template)
./scripts/deploy_personal_env.py

# Force ARM template redeploy (useful if resources were partially created)
./scripts/deploy_personal_env.py --force-arm-deploy

# Skip docker build (useful for subsequent deploys when only Python code changes)
./scripts/deploy_personal_env.py --skip-docker
```

### Post-Deployment: Grant Storage Access for Local Development

After deployment, if you want to run tasks locally, grant yourself access to the staging storage account:
```bash
# Get your principal ID
MY_PRINCIPAL=$(az ad signed-in-user show --query id -o tsv)

# Grant Storage Blob Data Reader on your staging storage account
az role assignment create \
  --assignee $MY_PRINCIPAL \
  --role "Storage Blob Data Reader" \
  --scope "/subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.Storage/storageAccounts/<storage-account>"
```

### Verifying the Deployment

Check that all components are running:
```bash
# Check function apps are running
az functionapp list -g <resource-group> --query "[].{name:name,state:state}" -o table

# Check container app jobs
az containerapp job list -g <resource-group> --query "[].{name:name,status:properties.provisioningState}" -o table

# Check deployer task executions
az containerapp job execution list \
  --name $(az containerapp job list -g <resource-group> --query "[?contains(name,'deployer')].name" -o tsv) \
  -g <resource-group> -o table
```

Logs are sent to **Datadog** (not to blob storage). Check https://app.datadoghq.com/logs and search for `source:azure`.

### Troubleshooting

**Deployer task failing with authorization errors:**
The deployer task needs "Storage Blob Data Reader" permission on your staging storage account. This should be granted automatically by the deploy script, but you can manually add it:
```bash
DEPLOYER_PRINCIPAL=$(az containerapp job show \
  --name <deployer-task-name> \
  --resource-group <resource-group> \
  --query "identity.principalId" -o tsv)

az role assignment create \
  --assignee $DEPLOYER_PRINCIPAL \
  --role "Storage Blob Data Reader" \
  --scope "/subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.Storage/storageAccounts/<storage-account>"
```

**Cannot view Container App Job logs in portal:**
If you see "Could not find a corresponding Log Analytics Workspace", use the CLI to check execution status, or run tasks locally to debug.


## Running control plane tasks locally

Once you have deployed the rest of the control plane in Azure (either as a personal env or otherwise), you can run the control plane tasks locally, which will interact with the Azure resources.

Note: This will target your current Azure subscription in the CLI, so ensure that is set properly (`az account set --subscription <subscription id>`).

```bash
./scripts/run_task.sh resources_task my_lfo_rg
```

To run the deployer task locally:
```bash
export SUBSCRIPTION_ID="<your-subscription-id>"
export RESOURCE_GROUP="<your-resource-group>"
export CONTROL_PLANE_REGION="eastus2"
export STORAGE_ACCOUNT_URL="https://<staging-storage-account>.blob.core.windows.net"
export AzureWebJobsStorage=$(az storage account show-connection-string \
  --name <control-plane-storage-account> \
  --resource-group <resource-group> \
  --query connectionString -o tsv)

cd control_plane
python -m tasks.deployer_task
```

## Running Tests

```bash
pytest ./control_plane
```

## Checking Code Coverage

```bash
coverage run -m pytest ./control_plane > /dev/null ; coverage report -m
```
