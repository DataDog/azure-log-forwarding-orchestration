#!/usr/bin/env bash
set -euxo pipefail

./scripts/arm-template/gen-template.py
./scripts/arm-template/upload.sh
open -u `./scripts/arm-template/gen-url.py`
