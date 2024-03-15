#!/usr/bin/env bash

set -euxo pipefail

# install pytest
pip install pytest==8.0.2

# run diagnostic_settings tests
cd diagnostic_settings_task
pip install -r requirements.txt
python -m pytest tests

# run resources_task tests
cd ../resources_task
pip install -r requirements.txt
python -m pytest tests
