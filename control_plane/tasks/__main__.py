# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# project
from tasks.diagnostic_settings_task import DIAGNOSTIC_SETTINGS_TASK_NAME
from tasks.resources_task import RESOURCES_TASK_NAME
from tasks.scaling_task import SCALING_TASK_NAME

TASKS = [
    RESOURCES_TASK_NAME,
    DIAGNOSTIC_SETTINGS_TASK_NAME,
    SCALING_TASK_NAME,
]

if __name__ == "__main__":  # pragma: no cover
    print(" ".join(TASKS))
