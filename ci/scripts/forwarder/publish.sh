#!/usr/bin/env bash

set -euxo pipefail

cd forwarder

docker build --tag 486234852809.dkr.ecr.us-east-1.amazonaws.com/lfo/forwarder:latest .
docker push 486234852809.dkr.ecr.us-east-1.amazonaws.com/lfo/forwarder:latest
docker tag 486234852809.dkr.ecr.us-east-1.amazonaws.com/lfo/forwarder:latest registry.ddbuild.io/lfo/forwarder:latest
docker push registry.ddbuild.io/lfo/forwarder:latest
