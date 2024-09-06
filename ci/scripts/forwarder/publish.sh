#!/usr/bin/env bash

set -euxo pipefail

cd forwarder

#docker build --tag 486234852809.dkr.ecr.us-east-1.amazonaws.com/lfo/forwarder:latest .
#docker push 486234852809.dkr.ecr.us-east-1.amazonaws.com/lfo/forwarder:latest
#docker tag 486234852809.dkr.ecr.us-east-1.amazonaws.com/lfo/forwarder:latest registry.ddbuild.io/lfo/forwarder:latest
docker buildx build --platform=linux/amd64 --label target=build registry.ddbuild.io/lfo/forwarder:latest --push
