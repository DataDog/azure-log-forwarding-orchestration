#!/usr/bin/env bash
# Uploads LFO ARM template files as blobs to public-facing storage account - https://ddazurelfo.core.blob.windows.net
# Run from LFO root folder

set -euxo pipefail

az login --identity

az storage container create --name templates --account-name ddazurelfo --auth-mode login --public-access blob 

az storage blob upload --account-name ddazurelfo --auth-mode login --container-name templates --file ./deploy/createUiDefinition.json --name createUiDefinition.json --overwrite 
az storage blob upload --account-name ddazurelfo --auth-mode login --container-name templates --file ./deploy/build/azuredeploy.json --name azuredeploy.json --overwrite 
