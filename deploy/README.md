# Deploy

The Bicep templates for deploying LFO are maintained in [integrations-management](https://github.com/DataDog/integrations-management/tree/main/azure/logging_install/bicep).

## Personal Environment

To deploy a personal environment, run from the repo root:

```bash
# FunctionApp env
./scripts/deploy_personal_env.py --function-apps --force-arm-deploy

# ContainerAppJob env
./scripts/deploy_personal_env.py --container-app-jobs
```

The FunctionApp script uses the Bicep templates from your local `integrations-management` repo (`~/dd/integrations-management`).

### Flags

| Flag | Modes | Description |
|------|-------|-------------|
| `--function-apps` | — | Deploy a FunctionApp env (mutually exclusive with `--container-app-jobs`) |
| `--container-app-jobs` | — | Deploy a ContainerAppJob env (mutually exclusive with `--function-apps`) |
| `--skip-docker` | both | Skip building and pushing Docker images |
| `--force-arm-deploy` | FunctionApp | Force ARM/Bicep deployment even if the resource group already exists |
| `--force-recreate` | ContainerAppJob | Force recreate Container App Jobs even if they already exist |
