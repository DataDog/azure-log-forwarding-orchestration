#!/usr/bin/env bash
set -euo pipefail

cd forwarder
go test -coverprofile=c.out ./...
go tool cover -html=c.out -o coverage.html
