#!/usr/bin/env bash

set -euxo pipefail

PATH="/venv/bin:$PATH"


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
    : "Building $task"
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
    cat ./config/requirements.txt >> ./dist/$task/requirements.txt
    zip ./dist/$task.zip ./dist/$task/*
    : "Built $task"
done

ls -la dist/*
