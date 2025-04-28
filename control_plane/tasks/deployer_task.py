# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from asyncio import gather, run
from json import dumps
from os import environ
from types import TracebackType
from typing import Self, cast

# 3p
from aiohttp import ClientSession
from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.web.v2024_04_01.aio import WebSiteManagementClient
from azure.storage.blob.aio import ContainerClient
from tenacity import RetryError, retry, retry_if_not_exception_type, stop_after_attempt

# project
from cache.common import InvalidCacheError, read_cache, write_cache
from cache.env import (
    CONTROL_PLANE_REGION_SETTING,
    RESOURCE_GROUP_SETTING,
    STORAGE_ACCOUNT_URL_SETTING,
    SUBSCRIPTION_ID_SETTING,
    VERSION_TAG_SETTING,
    get_config_option,
)
from cache.manifest_cache import (
    KEY_TO_ZIP,
    MANIFEST_FILE_NAME,
    PUBLIC_STORAGE_ACCOUNT_URL,
    TASKS_CONTAINER,
    ControlPlaneComponent,
    ManifestCache,
    deserialize_manifest_cache,
)
from tasks.common import (
    DIAGNOSTIC_SETTINGS_TASK_PREFIX,
    RESOURCES_TASK_PREFIX,
    SCALING_TASK_PREFIX,
    Resource,
    is_azure_gov,
)
from tasks.concurrency import collect
from tasks.task import Task, task_main

DEPLOYER_TASK_NAME = "deployer_task"


MAX_ATTEMPTS = 5
MAX_WAIT_TIME = 30


class DeployError(Exception):
    pass


def get_azure_mgmt_url(region: str) -> str:
    return "https://management." + ("usgovcloudapi.net" if is_azure_gov(region) else "azure.com")


class DeployerTask(Task):
    NAME = DEPLOYER_TASK_NAME

    def __init__(self) -> None:
        super().__init__()
        self.subscription_id = get_config_option(SUBSCRIPTION_ID_SETTING)
        self.resource_group = get_config_option(RESOURCE_GROUP_SETTING)
        self.region = get_config_option(CONTROL_PLANE_REGION_SETTING)
        storage_account_url = environ.get(STORAGE_ACCOUNT_URL_SETTING, PUBLIC_STORAGE_ACCOUNT_URL)
        self.public_storage_client = ContainerClient(storage_account_url, TASKS_CONTAINER)
        self.rest_client = ClientSession()
        self.web_client = WebSiteManagementClient(self.credential, self.subscription_id)

    async def __aenter__(self) -> Self:
        await super().__aenter__()
        await gather(
            self.public_storage_client.__aenter__(),
            self.rest_client.__aenter__(),
            self.web_client.__aenter__(),
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
            self.web_client.__aexit__(exc_type, exc_val, exc_tb),
        )
        await super().__aexit__(exc_type, exc_val, exc_tb)

    async def run(self) -> None:
        public_manifest, private_manifest, current_function_app_ids = await gather(
            self.get_public_manifests(), self.get_private_manifests(), self.get_current_function_apps()
        )
        if not private_manifest:
            self.log.info("Failed to read private manifest. Deploying all components.")
        self.public_manifest = public_manifest
        self.private_manifest = private_manifest
        self.manifest_cache = (
            private_manifest
            or {
                "resources": "",
                "scaling": "",
                "diagnostic_settings": "",
            }
        ).copy()

        await gather(
            *[
                self.deploy_component(component, current_function_app_ids)
                for component in public_manifest
                if not private_manifest or public_manifest[component] != private_manifest[component]
            ]
        )

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS), retry=retry_if_not_exception_type(InvalidCacheError))
    async def get_public_manifests(self) -> ManifestCache:
        try:
            stream = await self.public_storage_client.download_blob(MANIFEST_FILE_NAME)
        except ResourceNotFoundError as e:
            raise InvalidCacheError("Public Manifest not found") from e
        blob_data = await stream.readall()
        cache_str = blob_data.decode()
        if not (cache := deserialize_manifest_cache(cache_str)):
            raise InvalidCacheError(f"Invalid Public Manifest: {cache_str}")
        return cache

    async def get_private_manifests(self) -> ManifestCache | None:
        try:
            blob_data = await retry(stop=stop_after_attempt(MAX_ATTEMPTS))(read_cache)(MANIFEST_FILE_NAME)
        except RetryError as e:
            self.log.error("Error reading private manifest cache", exc_info=e.last_attempt.exception())
            return None
        return deserialize_manifest_cache(blob_data)

    async def get_current_function_apps(self) -> set[str]:
        current_apps = await collect(self.web_client.web_apps.list_by_resource_group(self.resource_group))
        return {
            task.name
            for task in cast(list[Resource], current_apps)
            if any(
                task.name.startswith(prefix)
                for prefix in (SCALING_TASK_PREFIX, RESOURCES_TASK_PREFIX, DIAGNOSTIC_SETTINGS_TASK_PREFIX)
            )
        }

    async def deploy_component(self, component: ControlPlaneComponent, current_function_app_ids: set[str]) -> None:
        task_prefix = f"{component.replace('_', '-')}-task-"
        function_app = next((app for app in current_function_app_ids if app.startswith(task_prefix)), None)
        if not function_app:
            self.log.error(f"Function app for {component} not found in {current_function_app_ids}, skipping deployment")
            return
        try:
            self.log.info(f"Downloading function app data for {component}")
            zip_data = await self.download_function_app_data(component)
            self.log.info(f"Deploying {function_app}")
            await self.upload_function_app_data(function_app, zip_data)
            await self.set_function_app_version(function_app, self.public_manifest[component])
            await self.sync_function_app_triggers(function_app)
        except Exception:
            self.log.exception(f"Failed to deploy {component}")
            try:
                await self.set_function_app_version(function_app, self.manifest_cache[component])
            except Exception:
                self.log.exception(f"Failed to rollback version variable for {function_app}")
            return
        self.manifest_cache[component] = self.public_manifest[component]
        self.log.info(f"Finished deploying {component}")

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def upload_function_app_data(self, function_app_name: str, function_app_data: bytes) -> None:
        function_app_url = "https://{}.scm.azurewebsites.{}/api/zipdeploy".format(
            function_app_name, "us" if is_azure_gov(self.region) else "net"
        )
        resp = await self.rest_client.post(function_app_url, data=function_app_data)
        if not resp.ok:
            content = (await resp.content.read()).decode()
            raise DeployError(f"Failed to upload function app data: {resp.status} ({resp.reason})\n{content}")

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def set_function_app_version(self, function_app_name: str, version: str) -> None:
        app_settings = await self.web_client.web_apps.list_application_settings(self.resource_group, function_app_name)
        if not app_settings.properties:
            app_settings.properties = {}
        app_settings.properties[VERSION_TAG_SETTING] = version
        await self.web_client.web_apps.update_application_settings(self.resource_group, function_app_name, app_settings)

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def sync_function_app_triggers(self, function_app_name: str) -> None:
        resp = await self.rest_client.post(
            f"{get_azure_mgmt_url(self.region)}/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}/providers/Microsoft.Web/sites/{function_app_name}/syncfunctiontriggers?api-version=2016-08-01"
        )
        if not resp.ok:
            content = (await resp.content.read()).decode()
            raise DeployError(f"Failed to sync function app triggers: {resp.status} ({resp.reason})\n{content}")

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def download_function_app_data(self, component: ControlPlaneComponent) -> bytes:
        blob_name = KEY_TO_ZIP[component]
        stream = await self.public_storage_client.download_blob(blob_name)
        app_data = await stream.readall()
        return app_data

    async def write_caches(self) -> None:
        if self.manifest_cache == self.private_manifest:
            return
        await write_cache(MANIFEST_FILE_NAME, dumps(self.manifest_cache))


if __name__ == "__main__":
    run(task_main(DeployerTask, []))
