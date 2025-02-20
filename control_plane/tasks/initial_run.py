# stdlib
from asyncio import run
from json import dumps
from logging import INFO, basicConfig

# 3p
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.appcontainers.aio import ContainerAppsAPIClient

# project
from cache.common import read_cache
from cache.env import CONTROL_PLANE_ID_SETTING, RESOURCE_GROUP_SETTING, SUBSCRIPTION_ID_SETTING, get_config_option
from tasks.diagnostic_settings_task import DiagnosticSettingsTask
from tasks.resources_task import RESOURCE_CACHE_BLOB, ResourcesTask
from tasks.scaling_task import ScalingTask

SUB_ID = get_config_option(SUBSCRIPTION_ID_SETTING)
RESOURCE_GROUP = get_config_option(RESOURCE_GROUP_SETTING)
CONTROL_PLANE_ID = get_config_option(CONTROL_PLANE_ID_SETTING)


async def start_deployer() -> None:
    async with DefaultAzureCredential() as cred, ContainerAppsAPIClient(cred, SUB_ID) as client:
        await client.jobs.begin_start(RESOURCE_GROUP, f"deployer-task-{CONTROL_PLANE_ID}")


async def is_initial_deploy() -> bool:
    return await read_cache(RESOURCE_CACHE_BLOB) == ""


async def run_tasks() -> None:
    basicConfig(level=INFO)
    async with ResourcesTask("") as resources_task:
        await resources_task.run()
        resource_cache = dumps(resources_task.resource_cache, default=list)
    async with ScalingTask(resource_cache, "", wait_on_envs=True) as scaling_task:
        await scaling_task.run()
        assignment_cache = dumps(scaling_task.assignment_cache)
    async with ScalingTask(resource_cache, assignment_cache) as scaling_task:
        await scaling_task.run()
        assignment_cache = dumps(scaling_task.assignment_cache)
    async with DiagnosticSettingsTask(assignment_cache, "") as diagnostic_settings_task:
        await diagnostic_settings_task.run()


async def main() -> None:
    if await is_initial_deploy():
        await run_tasks()
    else:
        print("This is a re-deploy, starting deployer instead")
    await start_deployer()


run(main())
