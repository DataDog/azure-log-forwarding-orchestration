# stdlib
from asyncio import run
from json import dumps

# project
from tasks.diagnostic_settings_task import DiagnosticSettingsTask
from tasks.resources_task import ResourcesTask
from tasks.scaling_task import ScalingTask


async def main() -> None:
    async with ResourcesTask("") as resources_task:
        await resources_task.run()
        resource_cache = dumps(resources_task.resource_cache, default=list)
    async with ScalingTask("", resource_cache, wait=True) as scaling_task:
        await scaling_task.run()
        assignment_cache = dumps(scaling_task.assignment_cache)
    async with ScalingTask(assignment_cache, resource_cache) as scaling_task:
        await scaling_task.run()
        assignment_cache = dumps(scaling_task.assignment_cache)
    async with DiagnosticSettingsTask(assignment_cache) as diagnostic_settings_task:
        await diagnostic_settings_task.run()


run(main())

# Start-AzContainerAppJob -Name ${deployerTaskName} -ResourceGroupName ${controlPlaneResourceGroupName}
