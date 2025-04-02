#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.


[ -f "/venv/bin/activate" ] && source /venv/bin/activate

set -euxo pipefail

[ -d "./dist" ] && rm -rf ./dist

cd ./control_plane

: Install dependencies just in case
pip install -e '.[dev]'

tasks="$(python -m tasks)"
echo Building the following tasks: "$tasks"
cd ..
for task in $tasks; do
    echo "Building $task"
    mkdir -p "./dist/$task/$task"

    : Compile "$task" into a single file
    stickytape "./control_plane/tasks/$task.py" \
        --add-python-path ./control_plane \
        --output-file "./dist/$task/$task/task.py"

    : requirements.txt
    python -c "import tomllib; project = tomllib.load(open('./control_plane/pyproject.toml','rb'))['project']; print('\n'.join(project['dependencies'] + project['optional-dependencies']['$task']))" \
        > "./dist/$task/requirements.txt"

    : entrypoint
    cp ./control_plane/config/__init__.py "./dist/$task/$task/__init__.py"

    : function.json
    cp "./control_plane/config/$task/function.json" "./dist/$task/$task/function.json"

    : host.json
    cp "./control_plane/config/host.json" "./dist/$task/host.json"

    : zip it up for zipdeploy
    cd "./dist/$task"
    zip -r "../$task.zip" ./*
    cd -
    echo "Built $task"
done

echo Building the initial run script...
python ci/scripts/control_plane/build_initial_run.py

: ======================= Done Building =======================
