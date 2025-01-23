# Getting Started with the Control Plane

All commands assume you are in the repository root.

## One time setup

Set up Local Dev Environment:
```bash
pyenv install 3.11.8
brew install pyenv-virtualenv
pyenv virtualenv 3.11.8 lfo
pyenv local lfo; pyenv shell lfo
pip install -e '.[dev]'
pre-commit install
```

Make sure you have the Azure CLI installed and are logged in.
```bash
brew install azure-cli
az login
```

## IDE Setup
For VSCode, install the [reccomended extensions](./control_plane/.vscode/extensions.json) which should be suggested by your IDE when you open the control_plane as the workspace.

For Pycharm, just install the [ruff plugin](https://plugins.jetbrains.com/plugin/20574-ruff).


## Deploying to a personal environment:
```bash
./scripts/deploy_personal_env.py
# to force an ARM template redeploy
./scripts/deploy_personal_env.py --force-arm-deploy
# to skip the docker and publish build step (required initially)
./scripts/deploy_personal_env.py --skip-docker
```

## Running control plane tasks locally
Once you have deployed the rest of the control plane in azure (either as a personal env or otherwise), you can run the control plane tasks locally, which will interact with the azure resources.

Note: This will target your current azure subscription in the cli, so ensure that is set properly (`az account set --subscription <subscription id>`).

```bash
./scripts/run_task.sh resources_task my_lfo_rg
```

## Running Tests

```bash
pytest ./control_plane
```

## Checking Code Coverage

```bash
coverage run -m pytest ./control_plane > /dev/null ; coverage report -m
```
