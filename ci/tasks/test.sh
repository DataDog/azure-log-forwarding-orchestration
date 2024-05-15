#!/usr/bin/env bash

set -euxo pipefail

# list all installed python packages
pip list

# run tests and coverage
python -m coverage run -m pytest ./src

# generate coverage report
python -m coverage report --skip-empty --show-missing > ci/coverage.txt
