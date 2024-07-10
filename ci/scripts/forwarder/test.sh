#!/usr/bin/env bash
set -euo pipefail

cd forwarder
go test -coverprofile=forwarder_coverage.txt -race -json -v ./...
/root/go/bin/gocover-cobertura < forwarder_coverage.txt > forwarder_coverage.xml

source /venv/bin/activate
pycobertura show --format markdown forwarder_coverage.xml > forwarder_coverage.md
mv forwarder_coverage.md ../ci/
