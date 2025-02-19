# stdlib
from asyncio import run
from json import dumps

# 3p
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.appcontainers.aio import ContainerAppsAPIClient

# project
from cache.env import CONTROL_PLANE_ID_SETTING, RESOURCE_GROUP_SETTING, SUBSCRIPTION_ID_SETTING, get_config_option
from tasks.diagnostic_settings_task import DiagnosticSettingsTask
from tasks.resources_task import ResourcesTask
from tasks.scaling_task import ScalingTask


async def start_deployer() -> None:
    sub_id = get_config_option(SUBSCRIPTION_ID_SETTING)
    resource_group = get_config_option(RESOURCE_GROUP_SETTING)
    control_plane_id = get_config_option(CONTROL_PLANE_ID_SETTING)
    deployer_name = f"deployer-task-{control_plane_id}"
    async with DefaultAzureCredential() as cred, ContainerAppsAPIClient(cred, sub_id) as client:
        await client.jobs.begin_start(resource_group, deployer_name)


async def main() -> None:
    async with ResourcesTask("") as resources_task:
        await resources_task.run()
        resource_cache = dumps(resources_task.resource_cache, default=list)
    async with ScalingTask("", resource_cache, wait_on_envs=True) as scaling_task:
        await scaling_task.run()
        assignment_cache = dumps(scaling_task.assignment_cache)
    async with ScalingTask(assignment_cache, resource_cache) as scaling_task:
        await scaling_task.run()
        assignment_cache = dumps(scaling_task.assignment_cache)
    async with DiagnosticSettingsTask(assignment_cache, "") as diagnostic_settings_task:
        await diagnostic_settings_task.run()
    await start_deployer()


run(main())
