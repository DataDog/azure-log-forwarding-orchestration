#!/usr/bin/env bash

source /venv/bin/activate

set -euxo pipefail

cd control_plane

: run mypy
python -m mypy ./control_plane
