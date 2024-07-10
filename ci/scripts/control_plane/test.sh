#!/usr/bin/env bash


source /venv/bin/activate

set -euxo pipefail

: run tests and coverage
python -m coverage run -m pytest ./control_plane

: generate coverage report
python -m coverage xml --skip-empty -o ci/control_plane_coverage.xml

pip install pycobertura
pycobertura show --format markdown ci/control_plane_coverage.xml > ci/control_plane_coverage.md
