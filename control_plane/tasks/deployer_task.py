# stdlib

from asyncio import gather, run
from collections.abc import Awaitable, Callable, Iterable
from json import dumps
from logging import INFO, basicConfig, getLogger
from types import TracebackType
from typing import Self, cast

# 3p
from aiohttp import ClientSession
from azure.core.exceptions import ResourceNotFoundError
from azure.core.polling import AsyncLROPoller
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
from tenacity import RetryError, retry, stop_after_attempt

# project
from cache.common import STORAGE_CONNECTION_SETTING, InvalidCacheError, get_config_option, read_cache, write_cache
from cache.manifest_cache import MANIFEST_CACHE_NAME, ManifestCache, ManifestKey, deserialize_manifest_cache
from tasks.common import collect, generate_unique_id, wait_for_resource
from tasks.task import Task

DEPLOYER_TASK_NAME = "deployer_task"

DEPLOYER_NAME = "control-plane-asp-9d911a05438d"
MAX_ATTEMPTS = 5
MAX_WAIT_TIME = 30

PUBLIC_CONTAINER_URL = "google.com"
APP_SERVICE_PLAN_PREFIX = "dd-lfo-control"

log = getLogger(DEPLOYER_NAME)


class DeployerTask(Task):
    def __init__(self) -> None:
        super().__init__()
        self.subscription_id = get_config_option("SUBSCRIPTION_ID")
        self.resource_group = get_config_option("RESOURCE_GROUP")
        self.region = get_config_option("REGION")
        self.control_plane_id = generate_unique_id()

        self.public_manifest_client = ContainerClient.from_container_url(PUBLIC_CONTAINER_URL)
        self.rest_client = ClientSession()
        self.web_client = WebSiteManagementClient(self.credential, self.subscription_id)

    async def __aenter__(self) -> Self:
        await super().__aenter__()
        await gather(
            self.public_manifest_client.__aenter__(), self.rest_client.__aenter__(), self.web_client.__aenter__()
        )
        token = await self.credential.get_token("https://management.azure.com/.default")
        self.rest_client.headers["Authorization"] = f"Bearer {token.token}"
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        await gather(
            self.public_manifest_client.__aexit__(exc_type, exc_val, exc_tb),
            self.rest_client.__aexit__(exc_type, exc_val, exc_tb),
            self.web_client.__aexit__(exc_type, exc_val, exc_tb),
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

        await gather(
            *[
                self.deploy_component(component)
                for component in cast(Iterable[ManifestKey], public_manifest)
                if not private_manifest or public_manifest[component] != private_manifest[component]
            ]
        )

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def get_public_manifests(self) -> ManifestCache:
        try:
            stream = await self.public_manifest_client.download_blob(MANIFEST_CACHE_NAME)
        except ResourceNotFoundError as e:
            raise InvalidCacheError("Public Manifest not found") from e
        blob_data = await stream.content_as_bytes(max_concurrency=4)
        cache_str = blob_data.decode()
        if not (cache := deserialize_manifest_cache(cache_str)):
            raise InvalidCacheError(f"Invalid Public Manifest: {cache_str}")
        return cache

    async def get_private_manifests(self) -> ManifestCache | None:
        try:
            blob_data = await retry(stop=stop_after_attempt(MAX_ATTEMPTS))(read_cache)(MANIFEST_CACHE_NAME)
        except RetryError as e:
            log.error("Error reading private manifest cache", exc_info=e.last_attempt.exception())
            return None
        return deserialize_manifest_cache(blob_data)

    async def get_current_components(self) -> list[str]:
        current_apps, current_service_plans = await gather(
            collect(self.web_client.web_apps.list_by_resource_group(self.resource_group)),
            collect(self.web_client.app_service_plans.list_by_resource_group(self.resource_group)),
        )
        return [app.name for app in current_apps if app.name.startswith(APP_SERVICE_PLAN_PREFIX)] + [  # type: ignore
            service_plan.name
            for service_plan in current_service_plans
            if service_plan.name.startswith(APP_SERVICE_PLAN_PREFIX)  # type: ignore
        ]

    async def deploy_component(self, component: ManifestKey) -> None:
        match component:
            case "forwarder":
                await self.deploy_log_forwarder_image()
            case "resources":
                await self.deploy_resources_task()
            case "scaling":
                await self.deploy_scaling_task()
            case "diagnostic_settings":
                await self.deploy_diagnostic_settings_task()
        self.manifest_cache[component] = self.public_manifest[component]

    async def deploy_log_forwarder_image(self) -> None:
        pass

    async def deploy_resources_task(self) -> None:
        pass

    async def deploy_scaling_task(self) -> None:
        pass

    async def deploy_diagnostic_settings_task(self) -> None:
        pass

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def deploy_function_app(self, function_app_name: str, service_plan: AppServicePlan, uuid_str: str) -> None:
        current_apps = self.web_client.web_apps.list_by_resource_group(self.resource_group)
        already_deployed = False
        async for app in current_apps:
            if app.name.startswith(function_app_name):  # type: ignore
                full_app_name: str = app.name  # type: ignore
                function_app_data = await self.download_function_app_data(function_app_name)
                await self.upload_function_app_data(full_app_name, function_app_data)
                already_deployed = True
                break
        if not already_deployed:
            _, function_app_data = await gather(
                self.create_or_update_function_app(function_app_name, uuid_str, service_plan),
                self.download_function_app_data(function_app_name),
            )
            await self.upload_function_app_data(
                self.get_full_function_app_name(function_app_name, uuid_str), function_app_data
            )

    async def create_or_update_function_app(
        self, function_app_name: str, uuid_str: str, service_plan: AppServicePlan
    ) -> None:
        await wait_for_resource(
            *await self.create_log_forwarder_function_app(
                self.region,
                self.get_full_function_app_name(function_app_name, uuid_str),
                service_plan.id,  # type: ignore
            )
        )

    def get_full_function_app_name(self, func_app_name_short: str, uuid_str: str) -> str:
        return func_app_name_short + uuid_str

    def generate_app_service_plan_name(self, uuid_str: str) -> str:
        return f"{APP_SERVICE_PLAN_PREFIX}{uuid_str}"

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def create_log_forwarder_app_service_plan(
        self, region: str, app_service_plan_name: str
    ) -> tuple[AsyncLROPoller[AppServicePlan], Callable[[], Awaitable[AppServicePlan]]]:
        return await self.web_client.app_service_plans.begin_create_or_update(
            self.resource_group,
            app_service_plan_name,
            AppServicePlan(
                location=region,
                kind="linux",
                reserved=True,
                sku=SkuDescription(
                    # TODO: figure out which SKU we should be using here
                    tier="Basic",
                    name="B1",
                ),
            ),
        ), lambda: self.web_client.app_service_plans.get(self.resource_group, app_service_plan_name)

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def create_log_forwarder_function_app(
        self, region: str, function_app_name: str, app_service_plan_id: str
    ) -> tuple[AsyncLROPoller[Site], Callable[[], Awaitable[Site]]]:
        connection_string = get_config_option(STORAGE_CONNECTION_SETTING)
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
            f"https://{function_app_name}.scm.azurewebsites.net/api/publish?type=zip",  # TODO [AZINTS-2705] Figure out how to get the right name here
            data=function_app_data,
        )
        resp.raise_for_status()

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def download_function_app_data(self, function_app_name: str) -> bytes:
        blob_name = function_app_name + "_task.zip"
        stream = await self.public_manifest_client.download_blob(blob_name)
        app_data = await stream.content_as_bytes(max_concurrency=4)
        return app_data

    async def write_caches(self) -> None:
        if self.manifest_cache == self.private_manifest:
            return
        await write_cache(MANIFEST_CACHE_NAME, dumps(self.manifest_cache))


async def main():
    async with DeployerTask() as deployer:
        await deployer.run()


if __name__ == "__main__":
    basicConfig(level=INFO)
    run(main())
