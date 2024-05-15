#!/usr/bin/env bash

set -euo pipefail

source /venv/bin/activate

set -x
: run mypy
python -m mypy ./src
