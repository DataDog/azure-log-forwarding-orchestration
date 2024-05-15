## Getting Started

### One time setup

<!-- Install the Core Tools package:
```bash
brew tap azure/functions
brew install azure-functions-core-tools@4
``` -->

Set up Local Dev Environment:
```bash
pyenv install 3.11.7
pyenv virtualenv 3.11.7 lfo
pip install -r requirements-dev.txt
find config/ -name 'requirements.txt' -exec pip install -r {} \;
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
