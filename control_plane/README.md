# Getting Started with the Control Plane

All commands assume you are in the repository root.

## One time setup

Set up Local Dev Environment:
```bash
pyenv install 3.12.3
brew install pyenv-virtualenv
pyenv virtualenv 3.12.3 lfo
pyenv local lfo
pip install -e '.[dev]'
pre-commit install
```


## Running Tests

Just run pytest:

```bash
pytest ./control_plane
```

## Checking Code Coverage

```bash
coverage run -m pytest ./control_plane > /dev/null ; coverage report -m
```

## Building and Deploying Function Apps Locally

### Building
```bash
./ci/scripts/control_plane/build_tasks.sh
```

### Deploying
Once you have built the tasks, you can deploy to the control plane as follows:

```bash
./scripts/deploy-control-plane.sh <control plane resource group>
```

Use the arm template to make the initial deploy, then use the `deploy-control-plane.sh` script to update the function apps.

DISCLAIMER: Using the script will break the deployer task, so only deploy to control plane instances you are using for testing.
