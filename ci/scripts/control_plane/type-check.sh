#!/usr/bin/env bash

source /venv/bin/activate

set -euxo pipefail

cd control_plane

: make sure all deps are installed
pip install -e '.[dev]'

: run mypy
python -m mypy .
