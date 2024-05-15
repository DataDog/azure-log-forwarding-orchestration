#!/usr/bin/env bash

source /venv/bin/activate

set -euxo pipefail

: run mypy
python -m mypy ./src
