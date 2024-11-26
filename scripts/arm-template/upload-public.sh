#!/usr/bin/env bash
# Uploads LFO ARM template files as blobs to public-facing storage account

set -euxo pipefail

az login --identity

az storage container create --name templates --account-name ddazurelfo --public-access blob --auth-mode login

az storage blob upload --container-name templates --file ./createUiDefinition.json --name createUiDefinition.json --overwrite
az storage blob upload --container-name templates --file ./build/azuredeploy.json --name azuredeploy.json --overwrite
