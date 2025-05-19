# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from asyncio import run
from json import dumps
from logging import INFO, basicConfig
from os import environ
from uuid import uuid4

# 3p
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.appcontainers.aio import ContainerAppsAPIClient

# project
from cache.common import read_cache
from cache.env import (
    CONTROL_PLANE_ID_SETTING,
    DD_API_KEY_SETTING,
    DD_SITE_SETTING,
    RESOURCE_GROUP_SETTING,
    SUBSCRIPTION_ID_SETTING,
    get_config_option,
)
from tasks.client.datadog_api_client import DatadogClient, StatusCode
from tasks.diagnostic_settings_task import DiagnosticSettingsTask
from tasks.resources_task import RESOURCE_CACHE_BLOB, ResourcesTask
from tasks.scaling_task import ScalingTask
from tasks.version import VERSION


async def start_deployer() -> None:
    async with (
        DefaultAzureCredential() as cred,
        ContainerAppsAPIClient(cred, get_config_option(SUBSCRIPTION_ID_SETTING)) as client,
    ):
        await client.jobs.begin_start(
            get_config_option(RESOURCE_GROUP_SETTING), f"deployer-task-{get_config_option(CONTROL_PLANE_ID_SETTING)}"
        )


async def is_initial_deploy() -> bool:
    return await read_cache(RESOURCE_CACHE_BLOB) == ""


async def run_tasks() -> None:
    basicConfig(level=INFO)
    execution_id = str(uuid4())
    control_plane_id = environ.get(CONTROL_PLANE_ID_SETTING)
    async with DatadogClient(environ.get(DD_SITE_SETTING), environ.get(DD_API_KEY_SETTING)) as datadog_client:
        await datadog_client.submit_status_update(
            "initial_run.begin", StatusCode.OK, "Starting initial run", execution_id, VERSION, control_plane_id
        )
        async with ResourcesTask("", is_initial_run=True, execution_id=execution_id) as resources_task:
            await resources_task.submit_status_update("task_start", StatusCode.OK, "Starting resources task")
            await resources_task.run()
            resource_cache = dumps(resources_task.resource_cache, default=list)
        async with ScalingTask(
            resource_cache, "", wait_on_envs=True, is_initial_run=True, execution_id=execution_id
        ) as scaling_task:
            await scaling_task.run()
            assignment_cache = dumps(scaling_task.assignment_cache)
        async with ScalingTask(
            resource_cache, assignment_cache, is_initial_run=True, execution_id=execution_id
        ) as scaling_task:
            await scaling_task.run()
            assignment_cache = dumps(scaling_task.assignment_cache)
        async with DiagnosticSettingsTask(
            resource_cache, assignment_cache, "", is_initial_run=True, execution_id=execution_id
        ) as diagnostic_settings_task:
            await diagnostic_settings_task.run()
        await datadog_client.submit_status_update(
            "initial_run.complete", StatusCode.OK, "Finished initial run", execution_id, VERSION, control_plane_id
        )


async def main() -> None:
    try:
        if await is_initial_deploy():
            await run_tasks()
        else:
            print("This is a re-deploy, starting deployer instead")
    finally:
        await start_deployer()


if __name__ == "__main__":
    # Set up logging
    basicConfig(level=INFO)
    # Run the main function
    run(main())
