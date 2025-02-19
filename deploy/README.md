# Arm Template

(this assumes your current directory is the root of the repo)

## Setup:

```bash
# ensure the az cli is installed
brew install azure-cli
# install the bicep cli
az bicep install
```

## Development:

To work with the bicep files, you can just make your changes and then run the
deploy personal environment script with --force-arm-deploy to deploy the changes:

```bash
./scripts/deploy_personal_env.py --force-arm-deploy
```
