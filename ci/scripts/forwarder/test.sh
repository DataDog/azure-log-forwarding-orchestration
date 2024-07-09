#!/usr/bin/env bash
set -euo pipefail

cd forwarder
go test -coverprofile=forwarder_coverage.txt ./...
mv forwarder_coverage.txt ../ci/
