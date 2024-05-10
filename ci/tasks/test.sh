#!/usr/bin/env bash

set -euxo pipefail

# install testing/coverage dependencies
pip install pytest==8.0.2 coverage==7.4.4

# install project dependencies
for file in config/*/requirements.txt; do
    pip install -r $file
done

# run tests and coverage
python -m coverage run -m pytest src

# generate coverage report
python -m coverage report --skip-empty --show-missing > ci/coverage.txt
