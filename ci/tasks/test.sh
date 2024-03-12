#!/usr/bin/env bash

set -euo pipefail

# install pytest
pip install pytest==8.0.2

# run diagnostic_settings tests
cd diagnostic_settings_task
pip install -r requirements.txt
python -m pytest tests

# TODO run log_forwarding tests
