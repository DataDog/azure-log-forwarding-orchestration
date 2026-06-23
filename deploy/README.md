# Deploy

The Bicep templates for deploying LFO are maintained in [integrations-management](https://github.com/DataDog/integrations-management/tree/main/azure/logging_install/bicep).

## Personal Environment

To deploy a personal environment, run from the repo root:

```bash
# Function App env (default)
./scripts/deploy_personal_env/deploy_personal_env.py --force-arm-deploy

# Container App Job env
./scripts/deploy_personal_env/deploy_personal_env.py --caj
```

The FunctionApp script uses the Bicep templates from your local `integrations-management` repo (`~/dd/integrations-management`).

### Flags

| Flag | Modes | Description |
|------|-------|-------------|
| `--caj` | — | Deploy a Container App Job env instead of a Function App env |
| `--skip-docker` | both | Skip building and pushing Docker images |
| `--force-arm-deploy` | FunctionApp | Force ARM/Bicep deployment even if the resource group already exists |
| `--force-recreate` | ContainerAppJob | Force recreate Container App Jobs even if they already exist |
