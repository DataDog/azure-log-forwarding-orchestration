# stdlib

from asyncio import gather, run
from collections.abc import Awaitable, Callable, Iterable
from json import dumps
from logging import INFO, basicConfig, getLogger
from types import TracebackType
from typing import NamedTuple, Self, cast

# 3p
from aiohttp import ClientSession
from azure.core.exceptions import ResourceNotFoundError
from azure.core.polling import AsyncLROPoller
from azure.mgmt.storage.v2023_05_01.aio import StorageManagementClient
from azure.mgmt.web.v2023_12_01.aio import WebSiteManagementClient
from azure.mgmt.web.v2023_12_01.models import (
    AppServicePlan,
    ManagedServiceIdentity,
    NameValuePair,
    Site,
    SiteConfig,
    SkuDescription,
)
from azure.storage.blob.aio import ContainerClient
from tenacity import RetryError, retry, retry_if_not_exception_type, stop_after_attempt

# project
from cache.common import InvalidCacheError, get_config_option, read_cache, write_cache
from cache.manifest_cache import (
    KEY_TO_ZIP,
    MANIFEST_FILE_NAME,
    PUBLIC_STORAGE_ACCOUNT_URL,
    TASKS_CONTAINER,
    ManifestCache,
    ManifestKey,
    deserialize_manifest_cache,
)
from tasks.client.log_forwarder_client import Resource
from tasks.common import collect, generate_unique_id
from tasks.deploy_common import wait_for_resource
from tasks.task import Task

DEPLOYER_TASK_NAME = "deployer_task"

CONTROL_PLANE_APP_SERVICE_PLAN_PREFIX = "dd-lfo-control-"
CONTROL_PLANE_STORAGE_PREFIX = "ddlfocontrol"
SCALING_TASK_PREFIX = "scaling-task-"
RESOURCES_TASK_PREFIX = "resources-task-"
DIAGNOSTIC_SETTINGS_TASK_PREFIX = "diagnostic-settings-task-"


MAX_ATTEMPTS = 5
MAX_WAIT_TIME = 30


log = getLogger(DEPLOYER_TASK_NAME)


class ControlPlaneResources(NamedTuple):
    app_service_plans: set[str]
    storage_accounts: set[str]
    function_apps: set[str]


class DeployError(Exception):
    pass


class DeployerTask(Task):
    def __init__(self) -> None:
        super().__init__()
        self.subscription_id = get_config_option("SUBSCRIPTION_ID")
        self.resource_group = get_config_option("RESOURCE_GROUP")
        self.region = get_config_option("REGION")
        self.control_plane_id = generate_unique_id()
        self.public_storage_client = ContainerClient(PUBLIC_STORAGE_ACCOUNT_URL, TASKS_CONTAINER)
        self.rest_client = ClientSession()
        self.web_client = WebSiteManagementClient(self.credential, self.subscription_id)
        self.storage_client = StorageManagementClient(self.credential, self.subscription_id)

    async def __aenter__(self) -> Self:
        await super().__aenter__()
        await gather(
            self.public_storage_client.__aenter__(),
            self.rest_client.__aenter__(),
            self.web_client.__aenter__(),
            self.storage_client.__aenter__(),
        )
        token = await self.credential.get_token("https://management.azure.com/.default")
        self.rest_client.headers["Authorization"] = f"Bearer {token.token}"
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        await gather(
            self.public_storage_client.__aexit__(exc_type, exc_val, exc_tb),
            self.rest_client.__aexit__(exc_type, exc_val, exc_tb),
            self.web_client.__aexit__(exc_type, exc_val, exc_tb),
            self.storage_client.__aexit__(exc_type, exc_val, exc_tb),
        )
        await super().__aexit__(exc_type, exc_val, exc_tb)

    async def run(self) -> None:
        public_manifest, private_manifest, current_components = await gather(
            self.get_public_manifests(), self.get_private_manifests(), self.get_current_components()
        )
        if not private_manifest:
            log.info("Failed to read private manifest. Deploying all components.")
        self.public_manifest = public_manifest
        self.private_manifest = private_manifest
        self.manifest_cache = (
            private_manifest
            or {
                "forwarder": "",
                "resources": "",
                "scaling": "",
                "diagnostic_settings": "",
            }
        ).copy()

        # TODO(AZINTS-2771) implement creating dependency resources when needed

        await gather(
            *[
                self.deploy_component(component, current_components)
                for component in cast(Iterable[ManifestKey], public_manifest)
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
            log.error("Error reading private manifest cache", exc_info=e.last_attempt.exception())
            return None
        return deserialize_manifest_cache(blob_data)

    async def get_current_components(self) -> ControlPlaneResources:
        current_apps, current_service_plans, storage_accounts = await gather(
            collect(self.web_client.web_apps.list_by_resource_group(self.resource_group)),
            collect(self.web_client.app_service_plans.list_by_resource_group(self.resource_group)),
            collect(self.storage_client.storage_accounts.list_by_resource_group(self.resource_group)),
        )
        return ControlPlaneResources(
            app_service_plans={
                app.name
                for app in cast(list[Resource], current_service_plans)
                if app.name.startswith(CONTROL_PLANE_APP_SERVICE_PLAN_PREFIX)
            },
            storage_accounts={
                storage.name
                for storage in cast(list[Resource], storage_accounts)
                if storage.name.startswith(CONTROL_PLANE_STORAGE_PREFIX)
            },
            function_apps={
                task.name
                for task in cast(list[Resource], current_apps)
                if any(
                    task.name.startswith(prefix)
                    for prefix in (SCALING_TASK_PREFIX, RESOURCES_TASK_PREFIX, DIAGNOSTIC_SETTINGS_TASK_PREFIX)
                )
            },
        )

    async def deploy_component(self, component: ManifestKey, current_components: ControlPlaneResources) -> None:
        log.info(f"Deploying {component}")
        if component == "forwarder":
            return await self.deploy_log_forwarder_image()
        task_prefix = f"{component.replace('_', '-')}-task-"
        function_app = next((app for app in current_components.function_apps if app.startswith(task_prefix)), None)
        if not function_app:
            log.error(
                f"Function app for {component} not found in {current_components.function_apps}, skipping deployment"
            )
            return
        try:
            log.info(f"Downloading function app data for {component}")
            zip_data = await self.download_function_app_data(component)
            log.info(f"Deploying {function_app}")
            await self.upload_function_app_data(function_app, zip_data)
        except Exception:
            log.exception(f"Failed to deploy {component}")
            return
        self.manifest_cache[component] = self.public_manifest[component]
        log.info(f"Finished deploying {component}")

    async def deploy_log_forwarder_image(self) -> None:
        # TODO(AZINTS-2770): Implement this
        self.manifest_cache["forwarder"] = self.public_manifest["forwarder"]

    async def create_or_update_function_app(
        self, function_app_name: str, service_plan: AppServicePlan, connection_string: str
    ) -> None:
        await wait_for_resource(
            *await self.create_control_plane_function_app(
                self.region,
                function_app_name,
                service_plan.id,  # type: ignore
                connection_string,
            )
        )

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def create_app_service_plan(
        self, region: str, app_service_plan_name: str
    ) -> tuple[AsyncLROPoller[AppServicePlan], Callable[[], Awaitable[AppServicePlan]]]:
        return await self.web_client.app_service_plans.begin_create_or_update(
            self.resource_group,
            app_service_plan_name,
            AppServicePlan(
                location=region,
                kind="linux",
                reserved=True,  # TODO(AZINTS-2646): figure out if this is what we want
                sku=SkuDescription(
                    # TODO(AZINTS-2646): figure out which SKU we should be using here
                    tier="Basic",
                    name="B1",
                ),
            ),
        ), lambda: self.web_client.app_service_plans.get(self.resource_group, app_service_plan_name)

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def create_control_plane_function_app(
        self, region: str, function_app_name: str, app_service_plan_id: str, connection_string: str
    ) -> tuple[AsyncLROPoller[Site], Callable[[], Awaitable[Site]]]:
        return await self.web_client.web_apps.begin_create_or_update(
            self.resource_group,
            function_app_name,
            Site(
                location=region,
                kind="functionapp",
                identity=ManagedServiceIdentity(type="SystemAssigned"),
                server_farm_id=app_service_plan_id,
                https_only=True,
                site_config=SiteConfig(
                    app_settings=[
                        NameValuePair(name="FUNCTIONS_WORKER_RUNTIME", value="python"),
                        NameValuePair(name="AzureWebJobsStorage", value=connection_string),
                        NameValuePair(name="FUNCTIONS_EXTENSION_VERSION", value="~4"),
                    ]
                ),
            ),
        ), lambda: self.web_client.web_apps.get(self.resource_group, function_app_name)

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def upload_function_app_data(self, function_app_name: str, function_app_data: bytes) -> None:
        resp = await self.rest_client.post(
            f"https://{function_app_name}.scm.azurewebsites.net/api/zipdeploy",
            data=function_app_data,
        )
        if not resp.ok:
            content = (await resp.content.read()).decode()
            raise DeployError(f"Failed to upload function app data: {resp.status} ({resp.reason})\n{content}")

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def download_function_app_data(self, component: str) -> bytes:
        blob_name = KEY_TO_ZIP[component]
        stream = await self.public_storage_client.download_blob(blob_name)
        app_data = await stream.readall()
        return app_data

    async def write_caches(self) -> None:
        if self.manifest_cache == self.private_manifest:
            return
        await write_cache(MANIFEST_FILE_NAME, dumps(self.manifest_cache))


async def main():
    async with DeployerTask() as deployer:
        await deployer.run()


if __name__ == "__main__":
    basicConfig(level=INFO)
    run(main())
