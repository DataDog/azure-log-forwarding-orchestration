#!/usr/bin/env bash

set -euo pipefail

cd ./control_plane

test_task() {
    local task_name=$1
    echo "Testing $task_name"
    python -m venv ./test_venv
    source ./test_venv/bin/activate

    pip install ".[$task_name]" >/dev/null

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
