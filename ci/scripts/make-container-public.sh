#!/usr/bin/env bash

source /venv/bin/activate

set -euxo pipefail

pip install azure-cli

connection=$(az storage account show-connection-string \
    --resource-group azints-us3-prod-dog \
    --name ddazurelfo \
    --query connectionString)

az storage container set-permission \
    --name tasks \
    --account-name ddazurelfo \
    --public-access container \
    --connection-string $connection
