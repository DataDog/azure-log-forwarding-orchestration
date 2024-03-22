#!/usr/bin/env bash

set -euxo pipefail

# install mypy
if [ "${CI:-}" == 'true' ]; then
    pip install mypy==1.9.0
    pip install -r diagnostic_settings_task/requirements.txt -r resources_task/requirements.txt
fi

python -m mypy diagnostic_settings_task resources_task
