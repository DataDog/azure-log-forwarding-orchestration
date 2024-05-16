#!/usr/bin/env bash

source /venv/bin/activate

set -euxo pipefail


for task in resources_task diagnostic_settings_task; do
    echo "Building $task"
    pyinstaller \
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
