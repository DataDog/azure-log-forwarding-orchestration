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

### Environment Variables

ContainerAppJob envs pass these optional variables from your shell to the control plane tasks. They are only applied
when a job is created, so use `--force-recreate` after changing them.

| Variable | Task | Description |
|----------|------|-------------|
| `RESOURCE_TAG_FILTERS` | resources | Comma-separated tag filters |
| `RESOURCE_PROVIDER_CONFIGS` | resources | JSON object with an optional `logs_enabled_default` and `providers` overrides |
| `PII_SCRUBBER_RULES` | scaling | PII scrubbing rules |

For example, to only forward logs from `Microsoft.Web` resources:

```bash
RESOURCE_PROVIDER_CONFIGS='{"logs_enabled_default":false,"providers":[{"namespace":"Microsoft.Web","logs_enabled":true}]}' \
  ./scripts/deploy_personal_env.py --container-app-jobs --force-recreate
```

FunctionApp envs are deployed from the `integrations-management` Bicep templates, which don't take
`RESOURCE_PROVIDER_CONFIGS` yet.
