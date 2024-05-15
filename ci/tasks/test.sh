#!/usr/bin/env bash

set -euxo pipefail

source /venv/bin/activate

: run tests and coverage
python -m coverage run -m pytest ./src

: generate coverage report
python -m coverage report --skip-empty --show-missing > ci/coverage.txt
