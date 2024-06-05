#!/usr/bin/env bash


source /venv/bin/activate

set -euxo pipefail

: run tests and coverage
python -m coverage run -m pytest ./src

: generate coverage report
python -m coverage report --skip-empty --show-missing > ci/coverage.txt
