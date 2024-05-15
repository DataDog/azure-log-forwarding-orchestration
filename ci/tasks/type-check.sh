#!/usr/bin/env bash

set -euxo pipefail

source /venv/bin/activate

: run mypy
python -m mypy diagnostic_settings_task resources_task
