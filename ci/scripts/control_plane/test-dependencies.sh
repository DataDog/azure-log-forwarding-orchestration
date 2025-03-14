#!/usr/bin/env bash

set -euo pipefail

cd ./control_plane
/venv/bin/pip install uv

test_task() {
    local task_name=$1
    echo "Testing $task_name. If there is an import error," \
        "the task is missing a dependency or is importing something incorrectly."
    /venv/bin/uv venv ./test_venv
    source ./test_venv/bin/activate

    /venv/bin/uv pip install ".[$task_name]" >/dev/null

    task_name_const="${task_name^^}_NAME"
    python -c "from tasks.$task_name import $task_name_const; print($task_name_const, 'successfully imported')"

    echo "Cleaning up..."
    deactivate
    rm -rf ./test_venv
}

for task in "resources_task" "scaling_task" "diagnostic_settings_task" "deployer_task"; do
    test_task "$task"
done

echo "All tasks have proper dependencies listed"
