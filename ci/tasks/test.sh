#!/usr/bin/env bash

set -euxo pipefail

# install pytest
pip install pytest==8.0.2

pip install -r diagnostic_settings_task/requirements.txt -r resources_task/requirements.txt
python -m pytest diagnostic_settings_task resources_task
