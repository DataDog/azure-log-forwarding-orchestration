#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.


set -euxo pipefail

cd forwarder

docker buildx build --platform=linux/amd64 --label target=build --build-arg VERSION_TAG="$3" --tag "$1/forwarder:$2" --push .
