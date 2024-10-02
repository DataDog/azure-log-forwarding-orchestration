#!/usr/bin/env bash

[ -f "/venv/bin/activate" ] && source /venv/bin/activate

set -euxo pipefail

[ -d "./dist" ] && rm -rf ./dist

cd ./control_plane

: Install dependencies just in case
pip install '.[dev]'

tasks="$(python -m tasks)"
echo Building the following tasks: $tasks
cd ..
for task in $tasks; do
    echo "Building $task"
    mkdir -p ./dist/$task/$task

    : Compile $task into a single file
    stickytape ./control_plane/tasks/$task.py \
        --add-python-path ./control_plane \
        --output-file ./dist/$task/$task/task.py

    : requirements.txt
    # TODO(AZINTS-2728): This is a hack to get the dependencies for the task. We should ideally only pull in the dependencies for the task
    python -c "import tomllib; print('\n'.join(tomllib.load(open('./control_plane/pyproject.toml','rb'))['project']['dependencies']))" \
        > ./dist/$task/requirements.txt

    : entrypoint
    cp ./control_plane/config/__init__.py ./dist/$task/$task/__init__.py

    : function.json
    cp ./control_plane/config/$task/function.json ./dist/$task/$task/function.json

    : host.json
    cp ./control_plane/config/host.json ./dist/$task/host.json

    : zip it up for zipdeploy
    cd ./dist/$task
    zip -r ../$task.zip *
    cd -
    echo "Built $task"
done

: ======================= Done Building =======================
