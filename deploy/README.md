# Deploy

The Bicep templates for deploying LFO are maintained in [integrations-management](https://github.com/DataDog/integrations-management/tree/main/azure/logging_install/bicep).

## Personal Environment

To deploy a personal environment, run:

```bash
./scripts/deploy_personal_env.py --force-arm-deploy
```

The script will use the Bicep templates from your local `integrations-management` repo (`~/dd/integrations-management`).