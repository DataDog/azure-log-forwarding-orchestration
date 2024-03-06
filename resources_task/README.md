## Getting Started

Install the Core Tools package:
```bash
brew tap azure/functions
brew install azure-functions-core-tools@4
```

Set up Local Dev Environment:
```bash
# first-time setup for all function apps
pyenv install 3.11.7
pyenv virtualenv 3.11.7 lfo
```

```bash
# for each lfo function app
pyenv local lfo
pip install -r requirements.txt
cp local.settings.example.json local.settings.json
```

If youve already run these commands^ before, but `lfo` isn't selected as your venv, you can run `pyenv local lfo` to have it automatically be set for the current directory.

## Publishing and Running
Publish to function app in azure:

Either in the command pallete "Azure Functions: Deploy to Function App..."

or via the cli:
```bash
func azure functionapp publish <function-app-name> --build remote 
```
