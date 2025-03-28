#!/usr/bin/env bash

set -euxo pipefail

cd forwarder

docker buildx build --platform=linux/amd64 --label target=build --build-arg VERSION_TAG="$3" --tag "$1/forwarder:$2" --push .
