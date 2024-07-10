#!/usr/bin/env bash

source /venv/bin/activate

set -euxo pipefail

tasks=($(cd control_plane; python3 -m tasks.__init__))

for task in $tasks; do
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
    cp ./control_plane/config/$task/function.json ./dist/$task/$task/function.json
    cp ./control_plane/config/host.json ./dist/$task/host.json
    # if AzureWebJobsStorage is set, then update it in the local settings file
    if [ -n "${AzureWebJobsStorage:-}" ]; then
        jq ".Values.AzureWebJobsStorage = \"$AzureWebJobsStorage\"" ./control_plane/config/local.settings.example.json > ./dist/$task/local.settings.json
    else
        cp ./control_plane/config/local.settings.example.json ./dist/$task/local.settings.json
    fi
    zip ./dist/$task.zip ./dist/$task/*
    echo "Built $task"
done

ls -la dist/*
