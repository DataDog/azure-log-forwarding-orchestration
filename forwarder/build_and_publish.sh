#!/usr/bin/env bash

set -euxo pipefail

docker buildx build . --platform=linux/amd64 --tag $1 --push
