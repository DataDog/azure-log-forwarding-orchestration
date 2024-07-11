#!/usr/bin/env bash

source /venv/bin/activate

set -euxo pipefail

: make sure all deps are installed
pip install -e './control_plane[dev]'

: run mypy
python -m mypy ./control_plane
