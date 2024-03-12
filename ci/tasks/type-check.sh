#!/usr/bin/env bash

set -euxo pipefail

# install mypy
pip install mypy==1.9.0
pip install -r diagnostic_settings_task/requirements.txt
pip install -r resources_task/requirements.txt

python -m mypy diagnostic_settings_task
python -m mypy resources_task
