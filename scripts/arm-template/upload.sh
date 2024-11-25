#!/usr/bin/env bash
set -euxo pipefail

# cd $HOME/dd/azure-log-forwarding-orchestration/deploy #altan - won't work in CI. you'll be in lfo folder already. cd into deploy

az storage blob upload --container-name templates --file ./createUiDefinition.json --name createUiDefinition.json --connection-string $connection --overwrite

az storage blob upload --container-name templates --file ./build/azuredeploy.json --name azuredeploy.json --connection-string $connection --overwrite
