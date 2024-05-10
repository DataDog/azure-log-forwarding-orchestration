#!/usr/bin/env bash

set -euo pipefail

mkdir -p build

pip install -U pyinstaller

tasks=(diagnostic_settings_task.py resources_task.py)

for task in $tasks; do
    pyinstaller src/tasks/$task

    exit

done
