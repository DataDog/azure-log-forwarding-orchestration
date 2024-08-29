#!/usr/bin/env bash

set -euxo pipefail

cd forwarder

: run tests with coverage
go test -coverprofile=forwarder_coverage.txt -race -json -v ./...

: generate coverage report
gocover-cobertura < forwarder_coverage.txt > forwarder_coverage.xml
python -m pycobertura show --format markdown forwarder_coverage.xml > ../ci/forwarder_coverage.md
