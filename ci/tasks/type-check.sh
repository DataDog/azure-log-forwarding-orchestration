#!/usr/bin/env bash

set -euxo pipefail

# install mypy
if [ "${CI:-}" == 'true' ]; then
    pip install mypy==1.9.0 -r config/requirements.txt
    for file in config/*/requirements.txt; do
        pip install -r $file
    done
fi

python -m mypy src
