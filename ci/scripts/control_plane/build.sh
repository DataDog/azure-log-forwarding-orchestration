#!/usr/bin/env bash

source /venv/bin/activate

set -euxo pipefail

[ -d "./dist" ] && rm -rf ./dist

cd ./control_plane
tasks="$(python3 -m tasks)"
echo Building the following tasks: $tasks
cd ..
for task in $tasks; do
    echo "Building $task"
    mkdir -p ./dist/$task/$task
    # TODO: This is a hack to get the dependencies for the task. We should ideally only pull in the dependencies for the task
    python -c "import toml; print('\n'.join(toml.load('./control_plane/pyproject.toml')['project']['dependencies']))" > ./dist/$task/requirements.txt
    stickytape ./control_plane/tasks/$task.py --add-python-path ./control_plane --output-file ./dist/$task/$task/task.py
    cp ./control_plane/config/__init__.py ./dist/$task/$task/__init__.py
    cp ./control_plane/config/$task/function.json ./dist/$task/$task/function.json
    cp ./control_plane/config/host.json ./dist/$task/host.json
    zip ./dist/$task.zip ./dist/$task/*
    echo "Built $task"
done

ls -la dist/*
