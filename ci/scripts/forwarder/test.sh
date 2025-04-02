#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.


set -euxo pipefail

cd forwarder

: run tests with coverage
go test -coverprofile=forwarder_coverage.txt -race -v ./...

: generate coverage report
gocover-cobertura < forwarder_coverage.txt > forwarder_coverage.xml
python -m pycobertura show --format markdown forwarder_coverage.xml > ../ci/forwarder_coverage.md
