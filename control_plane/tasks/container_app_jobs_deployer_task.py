# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from asyncio import gather, run
from os import environ
from types import TracebackType
from typing import Self, cast

# 3p
from aiohttp import ClientSession
from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.appcontainers.aio import ContainerAppsAPIClient
from azure.mgmt.appcontainers.models import (
    Container,
    Job,
    JobPatchProperties,
    JobPatchPropertiesProperties,
    JobTemplate,
)
from azure.storage.blob.aio import ContainerClient

# project
from cache.common import InvalidCacheError
from cache.env import (
    CONTROL_PLANE_REGION_SETTING,
    RESOURCE_GROUP_SETTING,
    STORAGE_ACCOUNT_URL_SETTING,
    SUBSCRIPTION_ID_SETTING,
    get_config_option,
)
from cache.manifest_cache import (
    PUBLIC_STORAGE_ACCOUNT_URL,
    TASK_IMAGES_MANIFEST_FILE_NAME,
    TASKS_CONTAINER,
    ControlPlaneComponent,
    ManifestCache,
    deserialize_manifest_cache,
)
from tasks.common import (
    DIAGNOSTIC_SETTINGS_TASK_PREFIX,
    RESOURCES_TASK_PREFIX,
    SCALING_TASK_PREFIX,
    get_azure_mgmt_url,
)
from tasks.concurrency import collect
from tasks.task import Task, task_main

CAJ_DEPLOYER_TASK_NAME = "container_app_jobs_deployer_task"

COMPONENT_TASK_PREFIXES: dict[ControlPlaneComponent, str] = {
    "resources": RESOURCES_TASK_PREFIX,
    "scaling": SCALING_TASK_PREFIX,
    "diagnostic_settings": DIAGNOSTIC_SETTINGS_TASK_PREFIX,
}


class DeployError(Exception):
    pass


class ContainerAppJobsDeployerTask(Task):
    NAME = CAJ_DEPLOYER_TASK_NAME

    def __init__(self, is_initial_run: bool = False) -> None:
        super().__init__()
        self.subscription_id = get_config_option(SUBSCRIPTION_ID_SETTING)
        self.resource_group = get_config_option(RESOURCE_GROUP_SETTING)
        self.region = get_config_option(CONTROL_PLANE_REGION_SETTING)
        self.rest_client = ClientSession()
        self.container_apps_client = ContainerAppsAPIClient(
            self.credential, self.subscription_id, base_url=get_azure_mgmt_url(self.region)
        )

        storage_account_url = environ.get(STORAGE_ACCOUNT_URL_SETTING, PUBLIC_STORAGE_ACCOUNT_URL)
        # If authenticating with the public storage account, we use anonymous access since the blobs are public.
        # In this case, we should not pass a credential to the ContainerClient.
        # If authenticating with a private storage account (ex. personal environment), we need to pass the credential
        # of the DeployerTask. It should have Storage Blob Data Contributor role (or similar) on the storage acocunt
        credential = self.credential if storage_account_url != PUBLIC_STORAGE_ACCOUNT_URL else None
        self.public_storage_client = ContainerClient(storage_account_url, TASKS_CONTAINER, credential=credential)

    async def __aenter__(self) -> Self:
        await super().__aenter__()
        await gather(
            self.public_storage_client.__aenter__(),
            self.rest_client.__aenter__(),
            self.container_apps_client.__aenter__(),
        )
        token_scope = get_azure_mgmt_url(self.region) + "/.default"
        token = await self.credential.get_token(token_scope)

        self.rest_client.headers["Authorization"] = f"Bearer {token.token}"
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        await gather(
            self.public_storage_client.__aexit__(exc_type, exc_val, exc_tb),
            self.rest_client.__aexit__(exc_type, exc_val, exc_tb),
            self.container_apps_client.__aexit__(exc_type, exc_val, exc_tb),
        )
        await super().__aexit__(exc_type, exc_val, exc_tb)

    async def run(self) -> None:
        public_manifest, current_control_plane_jobs = await gather(
            self.get_public_manifests(), self.get_control_plane_jobs()
        )

        components_to_update: dict[ControlPlaneComponent, tuple[Job, str]] = {
            component: (job, new_image)
            for component, new_image in public_manifest.items()
            if (job := current_control_plane_jobs.get(component)) is not None
            and self.get_current_image(component, job) != new_image
        }

        missing_components = public_manifest.keys() - current_control_plane_jobs.keys()
        for component in missing_components:
            self.log.error(f"Container app job for {component} not found, skipping update")

        if components_to_update:
            await gather(
                *[
                    self.update_task_image(component, job, new_image)
                    for component, (job, new_image) in components_to_update.items()
                ]
            )
        else:
            self.log.info("All components are up to date, skipping deployment")

    async def get_public_manifests(self) -> ManifestCache:
        try:
            stream = await self.public_storage_client.download_blob(TASK_IMAGES_MANIFEST_FILE_NAME)
        except ResourceNotFoundError as e:
            raise InvalidCacheError("Public Manifest not found") from e
        blob_data = await stream.readall()
        cache_str = blob_data.decode()
        if not (cache := deserialize_manifest_cache(cache_str)):
            raise InvalidCacheError(f"Invalid Public Manifest: {cache_str}")
        return cache

    async def get_control_plane_jobs(self) -> dict[ControlPlaneComponent, Job]:
        current_jobs = await collect(self.container_apps_client.jobs.list_by_resource_group(self.resource_group))
        jobs_by_component: dict[ControlPlaneComponent, Job] = {}
        for job in cast(list[Job], current_jobs):
            for component, prefix in COMPONENT_TASK_PREFIXES.items():
                if job.name and job.name.startswith(prefix):
                    jobs_by_component[component] = job
                    break
        return jobs_by_component

    def get_current_image(self, component: ControlPlaneComponent, job: Job) -> str | None:
        container_name = f"{component.replace('_', '-')}-task"
        containers = job.template.containers if job.template else None
        return next((container.image for container in containers or [] if container.name == container_name), None)

    async def update_task_image(self, component: ControlPlaneComponent, job: Job, new_image: str) -> None:
        container_name = f"{component.replace('_', '-')}-task"
        try:
            self.log.info(f"Updating image of {job.name}")
            await self.update_container_app_image(cast(str, job.name), container_name, new_image)
        except Exception:
            self.log.exception(f"Failed to update {component}")
            return
        self.log.info(f"Finished updating {component}")

    async def update_container_app_image(self, job_name: str, container_name: str, new_image: str) -> None:
        poller = await self.container_apps_client.jobs.begin_update(
            self.resource_group,
            job_name,
            JobPatchProperties(
                properties=JobPatchPropertiesProperties(
                    template=JobTemplate(containers=[Container(name=container_name, image=new_image)])
                )
            ),
        )
        await poller.result()

    async def write_caches(self) -> None:
        return


if __name__ == "__main__":
    run(task_main(ContainerAppJobsDeployerTask, []))
