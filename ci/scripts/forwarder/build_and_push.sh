#!/usr/bin/env bash

set -euxo pipefail

cd forwarder

docker buildx build --platform=linux/amd64 --label target=build --tag registry.ddbuild.io/lfo/forwarder:$1 --push .
