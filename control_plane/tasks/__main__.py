from tasks.resources_task import RESOURCES_TASK_NAME
from tasks.diagnostic_settings_task import DIAGNOSTIC_SETTINGS_TASK_NAME
from tasks.scaling_task import SCALING_TASK_NAME


TASKS = [
    RESOURCES_TASK_NAME,
    DIAGNOSTIC_SETTINGS_TASK_NAME,
    SCALING_TASK_NAME,
]

if __name__ == "__main__":
    print(" ".join(TASKS))
