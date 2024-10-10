#!/usr/bin/env bash

set -euo pipefail

cd ./control_plane
export UV_LINK_MODE=copy

greenify() {
    echo -e "\033[32m$@\033[0m"
}

test_task() {
    local task_name=$1
    greenify "Testing $task_name. If there is an import error," \
        "the task is missing a dependency or is importing something incorrectly."
    uv venv ./test_venv
    source ./test_venv/bin/activate

    uv pip install ".[$task_name]"

    task_name_const="${task_name^^}_NAME"
    greenify `python -c "from tasks.$task_name import $task_name_const; print($task_name_const, 'successfully imported')"`

    greenify "Cleaning up..."
    deactivate
    rm -rf ./test_venv
}

for task in "resources_task" "scaling_task" "diagnostic_settings_task" "deployer_task"; do
    test_task "$task"
done

greenify "All tasks have proper dependencies listed"
