#!/usr/bin/env bash


set -euo pipefail

source /venv/bin/activate

set -x

if [ "${CI:-}" == 'true' ]; then
    run-installer() {
        pyinstaller $@
    }
else
    run-installer() {
        eval "docker run -v \"$(pwd):/src/\" cdrx/pyinstaller-linux \"pyinstaller $@\""
    }
fi

for task in resources_task diagnostic_settings_task; do
    echo "Building $task"
    run-installer \
        --onefile \
        --noconfirm \
        --nowindow \
        --clean \
        --distpath ./dist/$task/ \
        --specpath ./specs \
        --log-level INFO \
        --paths ./src \
        ./src/tasks/$task.py
    cp ./config/$task/* ./dist/$task/
    cp ./config/host.json ./dist/$task/
    zip ./dist/$task.zip ./dist/$task/*
    echo "Built $task"
done

ls -la dist/*
