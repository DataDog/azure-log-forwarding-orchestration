#!/usr/bin/env bash
# Build bicep file to generate ARM JSON template

set -euxo pipefail

cd deploy
mkdir -p build
az bicep build -f azuredeploy.bicep --outdir ./build