#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.


source /venv/bin/activate

set -euxo pipefail

cd control_plane

: make sure all deps are installed
pip install -e '.[dev]'

: run pyright
python -m pyright --project pyproject.toml .
