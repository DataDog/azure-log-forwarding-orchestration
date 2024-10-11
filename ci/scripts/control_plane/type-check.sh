#!/usr/bin/env bash

source /venv/bin/activate

set -uxo pipefail

cd control_plane

: make sure all deps are installed
uv pip install '.[dev]'

: run mypy
python -m mypy --config-file pyproject.toml .
