from tasks.diagnostic_settings_task import DIAGNOSTIC_SETTINGS_TASK_NAME
from tasks.resources_task import RESOURCES_TASK_NAME
from tasks.scaling_task import SCALING_TASK_NAME

for task in (RESOURCES_TASK_NAME, DIAGNOSTIC_SETTINGS_TASK_NAME, SCALING_TASK_NAME):
    print(f"hello {task}")
# Start-AzContainerAppJob -Name ${deployerTaskName} -ResourceGroupName ${controlPlaneResourceGroupName}
