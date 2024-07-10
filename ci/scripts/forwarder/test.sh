#!/usr/bin/env bash
set -euo pipefail

cd forwarder
go test -coverprofile=forwarder_coverage.txt -race -json -v ./...
go get -v -u github.com/boumenot/gocover-cobertura
go install github.com/boumenot/gocover-cobertura
gocover-cobertura < forwarder_coverage.txt > forwarder_coverage.xml

pip install pycobertura
pycobertura show --format markdown forwarder_coverage.xml > forwarder_coverage.md
mv forwarder_coverage.md ../ci/
