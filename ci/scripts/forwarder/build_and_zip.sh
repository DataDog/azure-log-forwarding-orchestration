#!/usr/bin/env bash

set -euxo pipefail

# Create the dist directory if it doesn't exist
mkdir -p dist/forwarder/logforwarder

cd forwarder
# Build the Go program
export CGO_ENABLED=0
export GOOS=linux
go build -o ../dist/forwarder cmd/forwarder/forwarder.go

# Copy config into the dist directory
cp ./config/host.json ../dist/forwarder/
cp ./config/logforwarder/function.json ../dist/forwarder/logforwarder

cd ../dist/forwarder

# Zip the dist directory
zip -r ../forwarder.zip ./*
cd ../..
