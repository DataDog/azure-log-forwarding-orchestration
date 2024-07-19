## Getting Started

### One time setup

<!-- Install the Core Tools package:
```bash
brew tap azure/functions
brew install azure-functions-core-tools@4
``` -->

Set up Local Dev Environment:
```bash
pyenv install 3.12.3
brew install pyenv-virtualenv
pyenv virtualenv 3.12.3 lfo
pyenv local lfo
pip install -e '.[dev]'
pre-commit install
```

<!-- ### For each function app

```bash
cp local.settings.example.json local.settings.json
```

## Publishing and Running
Publish to function app in azure:

Either in the command pallete "Azure Functions: Deploy to Function App..."

or via the cli:
```bash
func azure functionapp publish <function-app-name> --build remote
``` -->


## Running Tests

Just run pytest:

```bash
pytest .
```

## Checking Code Coverage

```bash
coverage run -m pytest . > /dev/null ; coverage report -m
```

## Building and Deploying Function Apps Locally

### Building
```bash
cd ~/dd/azure-log-forwarding-orchestration
docker run -v "$(pwd):/src" registry.ddbuild.io/ci/azure-log-forwarding-offering-build:latest bash -c "cd /src/; AzureWebJobsStorage='DefaultEndpointsProtocol=https;...<the rest of your connection string>...' ./ci/scripts/control_plane/build.sh"
```

### Deploying
Currently the main known happy path is to use the azure functions cli (`brew install azure-functions-core-tools@4`):

```bash
cd ~/dd/azure-log-forwarding-orchestration/dist/
cd '<the function app you want to deploy, eg: resources_task>'
func azure functionapp publish your-function-name
```

Note: There are other methods to deploy but you may end up banging your head against a wall so be warned.
