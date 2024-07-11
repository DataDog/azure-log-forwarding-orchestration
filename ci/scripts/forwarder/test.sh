#!/usr/bin/env bash
set -euo pipefail

cd forwarder
go test -coverprofile=forwarder_coverage.txt -race -json -v ./...
go get -v -u github.com/boumenot/gocover-cobertura
go install github.com/boumenot/gocover-cobertura
/root/go/bin/gocover-cobertura < forwarder_coverage.txt > forwarder_coverage.xml

source /venv/bin/activate
python -m pycobertura show --format markdown forwarder_coverage.xml > ../ci/forwarder_coverage.md
