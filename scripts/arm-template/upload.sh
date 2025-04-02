#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

set -euxo pipefail

cd "$HOME/dd/azure-log-forwarding-orchestration/deploy"

az storage blob upload --container-name templates --file ./createUiDefinition.json --name createUiDefinition.json --connection-string "$connection" --overwrite

az storage blob upload --container-name templates --file ./build/azuredeploy.json --name azuredeploy.json --connection-string "$connection" --overwrite
