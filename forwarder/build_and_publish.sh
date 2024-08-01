#!/usr/bin/env bash

set -euxo pipefail

docker buildx build . --platform=linux/amd64 --tag  mattlogger.azurecr.io/forwarder:latest --push
