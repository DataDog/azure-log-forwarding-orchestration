#!/usr/bin/env bash


source /venv/bin/activate

set -euxo pipefail

: make sure all deps are installed
uv pip install -e './control_plane[dev]'

: run tests and coverage
python -m coverage run -m pytest ./control_plane

: generate coverage report
python -m coverage xml --skip-empty -o ci/control_plane_coverage.xml

: generate coverage report in markdown
python -m pycobertura show --format markdown ci/control_plane_coverage.xml | grep -v '100.00%' > ci/control_plane_coverage.md
