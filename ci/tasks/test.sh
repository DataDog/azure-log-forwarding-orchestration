#!/usr/bin/env bash

set -euxo pipefail

# install pytest
pip install pytest==8.0.2 coverage==7.4.4

pip install -r diagnostic_settings_task/requirements.txt -r resources_task/requirements.txt

python -m coverage run -m pytest diagnostic_settings_task resources_task

python -m coverage report > ci/coverage.txt
