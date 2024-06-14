#!/usr/bin/env bash

source /venv/bin/activate

set -euxo pipefail


for task in resources_task diagnostic_settings_task; do
    echo "Building $task"
    mkdir -p ./dist/$task/$task
    pyinstaller \
        --onefile \
        --noconfirm \
        --nowindow \
        --clean \
        --distpath ./dist/$task/bin \
        --specpath ./specs \
        --log-level INFO \
        --paths ./control_plane \
        ./control_plane/tasks/$task.py
    cp ./dist/$task/bin/$task ./dist/$task/main
    rm -rf ./dist/$task/bin
    cp ./config/$task/function.json ./dist/$task/$task/function.json
    cp ./config/host.json ./dist/$task/host.json
    # if AzureWebJobsStorage is set, then update it in the local settings file
    if [ -n "${AzureWebJobsStorage:-}" ]; then
        jq ".Values.AzureWebJobsStorage = \"$AzureWebJobsStorage\"" ./config/local.settings.example.json > ./dist/$task/local.settings.json
    else
        cp ./config/local.settings.example.json ./dist/$task/local.settings.json
    fi
    zip ./dist/$task.zip ./dist/$task/*
    echo "Built $task"
done

ls -la dist/*
