# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from asyncio import gather, run
from dataclasses import asdict, replace
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
    get_config_option,
)
from cache.manifest_cache import (
    KEY_TO_ZIP,
    MANIFEST_FILE_NAME,
    PUBLIC_STORAGE_ACCOUNT_URL,
    TASKS_CONTAINER,
    ComponentState,
    ControlPlaneComponent,
    ManifestCache,
    PrivateManifestCache,
    deserialize_private_manifest_cache,
    deserialize_public_manifest_cache,
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

KUDU_STATUSES_IN_PROGRESS = {0, 1, 2}
KUDU_STATUS_FAILED = 3
KUDU_STATUS_SUCCESS = 4

MAX_ATTEMPTS = 5
MAX_WAIT_TIME = 30


class DeployError(Exception):
    pass


def get_azure_mgmt_url(region: str) -> str:
    return "https://management." + ("usgovcloudapi.net" if is_azure_gov(region) else "azure.com")


def scm_url(region: str, function_app_name: str) -> str:
    domain = "us" if is_azure_gov(region) else "net"
    return f"https://{function_app_name}.scm.azurewebsites.{domain}"


class DeployerTask(Task):
    NAME = DEPLOYER_TASK_NAME

    def __init__(self, is_initial_run: bool = False) -> None:
        super().__init__(is_initial_run=is_initial_run)
        self.subscription_id = get_config_option(SUBSCRIPTION_ID_SETTING)
        self.resource_group = get_config_option(RESOURCE_GROUP_SETTING)
        self.region = get_config_option(CONTROL_PLANE_REGION_SETTING)
        self.rest_client = ClientSession()
        self.web_client = WebSiteManagementClient(self.credential, self.subscription_id)

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
        # TODO can the deployer task update the retry values of itself?

        public_manifest, private_manifest, current_function_app_ids = await gather(
            self.get_public_manifests(), self.get_private_manifests(), self.get_current_function_apps()
        )
        if not private_manifest:
            self.log.info("Failed to read private manifest. Deploying all components.")
        self.public_manifest = public_manifest
        self.private_manifest: PrivateManifestCache | None = private_manifest
        self.manifest_cache: PrivateManifestCache = (
            {k: replace(v) for k, v in private_manifest.items()}
            if private_manifest
            else {
                "resources": ComponentState(version="", deployment_url=""),
                "scaling": ComponentState(version="", deployment_url=""),
                "diagnostic_settings": ComponentState(version="", deployment_url=""),
            }
        )

        # TODO everywhere raise exception instead of just logging
        resource_task = next((app for app in current_function_app_ids if app.startswith(RESOURCES_TASK_PREFIX)), None)
        if not resource_task:
            self.log.error("Resources task function app not found, will not deploy")
            return

        await gather(
            *[
                self.deploy_component(component, current_function_app_ids, resource_task)
                for component in public_manifest
                if not private_manifest
                or public_manifest[component] != private_manifest[component].version
                or private_manifest[component].deployment_url
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
        if not (cache := deserialize_public_manifest_cache(cache_str)):
            raise InvalidCacheError(f"Invalid Public Manifest: {cache_str}")
        return cache

    async def get_private_manifests(self) -> PrivateManifestCache | None:
        try:
            blob_data = await retry(stop=stop_after_attempt(MAX_ATTEMPTS))(read_cache)(MANIFEST_FILE_NAME)
        except RetryError as e:
            self.log.error("Error reading private manifest cache", exc_info=e.last_attempt.exception())
            return None
        return deserialize_private_manifest_cache(blob_data)

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

    async def fix_content_share(self, function_app_name: str, resources_task_name: str) -> bool:
        settings = await self.web_client.web_apps.list_application_settings(
            self.resource_group, function_app_name
        )
        properties = settings.properties or {}

        current_value = properties.get("WEBSITE_CONTENTSHARE")
        if current_value != resources_task_name:
            # Variable has already been updated or was created via the python deployment script
            return False

        new_value = f"contentshare-{function_app_name}"
        self.log.info(
            "Fixing WEBSITE_CONTENTSHARE for %s (was: %s, now: %s)",
            function_app_name,
            properties.get("WEBSITE_CONTENTSHARE"),
            new_value,
        )
        if settings.properties is None:
            settings.properties = {}
        settings.properties["WEBSITE_CONTENTSHARE"] = new_value
        await self.web_client.web_apps.update_application_settings(
            self.resource_group, function_app_name, settings
        )
        return True

    async def deploy_component(self, component: ControlPlaneComponent, current_function_app_ids: set[str], resources_task_name: str) -> None:
        task_prefix = f"{component.replace('_', '-')}-task-"
        function_app = next((app for app in current_function_app_ids if app.startswith(task_prefix)), None)
        if not function_app:
            self.log.error(f"Function app for {component} not found in {current_function_app_ids}, skipping deployment")
            return

        try:
            content_share_fixed = await self.fix_content_share(function_app, resources_task_name)
        except Exception:
            self.log.exception("Failed to check/fix content share for %s", function_app)
            return
        if content_share_fixed:
            self.log.info("Content share fixed for %s, skipping deployment this run", function_app)
            return

        if self.manifest_cache[component].deployment_url:
            self.log.info("Found in-flight deployment for %s, polling deployment status", component)
            return await self.handle_ongoing_deployment(component, function_app)
        
        self.log.info("No in-flight deployment for %s, starting new deployment", component)
        return await self.start_async_deployment(component, function_app)

    async def handle_ongoing_deployment(self, component: ControlPlaneComponent, function_app: str) -> None:
        component_state = self.manifest_cache[component]
        try:
            status = await self.get_deployment_status(component_state.deployment_url)
        except (DeployError, RetryError):
            # Not sure what we should do
            self.log.exception("Failed to check deployment status for %s", component)
            return

        if status in KUDU_STATUSES_IN_PROGRESS:
            self.log.info("Deployment still in progress for %s (status: %i)", function_app, status)
            return  # Preserve version and deployment_url, next run will check deployment again

        if status == KUDU_STATUS_FAILED:
            self.log.error("Deployment failed for %s, will retry next run", component)
            component_state.deployment_url = ""
            return  # Next run sees version mismatch, no deployment_url -> re-deploys

        try:
            await self.sync_function_app_triggers(function_app)
        except Exception as e:
            self.log.exception("Failed to sync triggers for %s: %s", component, e)
            return  # Preserve version and deployment_url, next run will check deployment and sync triggers again

        component_state.version = self.public_manifest[component]
        component_state.deployment_url = ""
        self.log.info("Finished deploying %s", component)

    @retry(stop=stop_after_attempt(3))
    async def get_deployment_status(self, status_url: str) -> int:
        """Returns Kudu deployment status code. Raises DeployError on HTTP error or missing status."""
        resp = await self.rest_client.get(status_url)
        if not resp.ok:
            content = (await resp.content.read()).decode()
            raise DeployError(f"Failed to poll deployment status: {resp.status} ({resp.reason})\n{content}")
        body = await resp.json()
        status: int | None = body.get("status")
        if status is None:
            raise DeployError(f"Invalid response from deployment status endpoint: {body}")
        return status

    async def start_async_deployment(self, component: ControlPlaneComponent, function_app: str) -> None:
        try:
            zip_data = await self.download_function_app_data(component)
            poll_url = await self.upload_function_app_data(function_app, zip_data)
        except Exception as e:
            self.log.exception("Failed to start deployment for %s: %s", component, e)
            return
        # Update deployment URL, but keep version -> next run will poll deployment status instead of starting new deployment
        self.manifest_cache[component].deployment_url = poll_url
        self.log.info("Async deploy started for %s, will poll on next run", function_app)

    async def upload_function_app_data(self, function_app_name: str, function_app_data: bytes) -> str:
        # Don't retry the zip deploy to avoid starting multiple deployments
        function_app_url = scm_url(self.region, function_app_name) + "/api/zipdeploy?isAsync=true"
        resp = await self.rest_client.post(function_app_url, data=function_app_data)
        if resp.status != 202:
            content = (await resp.content.read()).decode()
            raise DeployError(f"Failed to start async zip deploy: expected 202, got {resp.status} ({resp.reason})\n{content}")
        poll_url = resp.headers.get("Location")
        if not poll_url:
            raise DeployError("Async zip deploy returned 202 but no Location header")
        return poll_url

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
        if self.manifest_cache != self.private_manifest:
            serialized = {
                k: {kk: vv for kk, vv in asdict(v).items() if kk != "deployment_url" or vv}
                for k, v in self.manifest_cache.items()
            }
            await write_cache(MANIFEST_FILE_NAME, dumps(serialized))


if __name__ == "__main__":
    run(task_main(DeployerTask, []))
