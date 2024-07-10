#!/usr/bin/env bash
set -euo pipefail

cd forwarder
go test -coverprofile=forwarder_coverage.txt -race -json -v ./...
go get github.com/boumenot/gocover-cobertura
/root/go/pkg/mod/cache/download/github.com/boumenot/gocover-cobertura < forwarder_coverage.txt > forwarder_coverage.xml
mv forwarder_coverage.xml ../ci/
