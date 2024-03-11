#!/usr/bin/env bash

set -euo pipefail

# image_tag=$(cat ci/image_tag.txt)
# readonly image_tag

# time docker run $image_tag

pip install -r build_requirements.txt

cd diagnostic_settings_task
pip install -r requirements.txt
python -m pytest tests
