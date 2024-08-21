# stdlib

from asyncio import gather, run
from collections.abc import Awaitable, Callable
from copy import deepcopy
from json import dumps
from logging import DEBUG, INFO, basicConfig, getLogger
from types import TracebackType
from typing import Self

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
from cache.common import STORAGE_CONNECTION_SETTING, get_config_option, read_cache, write_cache
from cache.manifest_cache import MANIFEST_CACHE_NAME, ManifestCache, deserialize_manifest_cache
from tasks.common import wait_for_resource
from tasks.task import Task

DEPLOYER_TASK_NAME = "deployer_task"

DEPLOYER_NAME = "control_plane_deployer"
MAX_ATTEMPTS = 5
MAX_WAIT_TIME = 30

PUBLIC_CONTAINER_URL = "google.com"

log = getLogger(DEPLOYER_NAME)
log.setLevel(DEBUG)


class DeployerTask(Task):
    def __init__(self) -> None:
        super().__init__()
        self.subscription_id = get_config_option("CONTROL_PLANE_SUB_ID")
        self.resource_group = get_config_option("RESOURCE_GROUP")
        self.region = get_config_option("REGION")

        self.uuid = get_config_option("UUID")
        self.uuid_parts = self.uuid.split("-")
        self.manifest_cache: ManifestCache = {}
        self.original_manifest_cache: ManifestCache = {}
        self.public_manifest: ManifestCache = {}
        self.public_client = ContainerClient.from_container_url(PUBLIC_CONTAINER_URL)
        self.rest_client = ClientSession()
        self.web_client = WebSiteManagementClient(self.credential, self.subscription_id)

    async def __aenter__(self) -> Self:
        await super().__aenter__()
        await self.public_client.__aenter__()
        await self.rest_client.__aenter__()
        await self.web_client.__aenter__()
        token = await self.credential.get_token("https://management.azure.com/.default")
        self.rest_client.headers["Authorization"] = f"Bearer {token.token}"
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        await self.public_client.__aexit__(exc_type, exc_val, exc_tb)
        await self.rest_client.__aexit__(exc_type, exc_val, exc_tb)
        await self.web_client.__aexit__(exc_type, exc_val, exc_tb)
        await super().__aexit__(exc_type, exc_val, exc_tb)

    async def run(self) -> None:
        public_manifest: dict[str, str] = {}
        private_manifest: dict[str, str] = {}
        public_manifest, private_manifest = await gather(self.get_public_manifests(), self.get_private_manifests())
        if len(public_manifest) == 0:
            log.error("Failed to read public manifests, exiting...")
            return
        if len(private_manifest) == 0:
            log.warn("Failed to read private manifests. Manifests may not exist or error may have occured.")
        self.manifest_cache = deepcopy(private_manifest)
        self.original_manifest_cache = private_manifest
        self.public_manifest = public_manifest
        await self.deploy_components(
            [
                component
                for component in public_manifest
                if public_manifest[component] != private_manifest.get(component)
            ]
        )

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def get_public_manifests(self) -> ManifestCache:
        try:
            stream = await self.public_client.download_blob(MANIFEST_CACHE_NAME)
        except ResourceNotFoundError:
            return {}
        blob_data = await stream.content_as_bytes(max_concurrency=4)
        return deserialize_manifest_cache(blob_data.decode()) or {}

    async def get_private_manifests(self) -> ManifestCache:
        try:
            blob_data = await retry(stop=stop_after_attempt(MAX_ATTEMPTS))(read_cache)(MANIFEST_CACHE_NAME)
        except RetryError:
            return {}
        return deserialize_manifest_cache(blob_data) or {}

    async def deploy_components(self, component_names: list[str]) -> None:
        if len(component_names) == 0:
            return
        container_apps: list[str] = []
        function_apps: list[str] = []
        for component in component_names:
            if component == "forwarder":
                container_apps.append(component)
            else:
                function_apps.append(component)
        await gather(self.deploy_function_apps(function_apps), self.deploy_container_apps(container_apps))

    async def deploy_function_apps(self, function_app_names: list[str]) -> None:
        if len(function_app_names) == 0:
            return
        await gather(*[self.deploy_function_app(function_app_name) for function_app_name in function_app_names])

    async def deploy_container_apps(self, container_app_names: list[str]) -> None:
        if len(container_app_names) == 0:
            return
        for container_app in container_app_names:
            self.manifest_cache[container_app] = self.public_manifest[container_app]

    async def deploy_function_app(self, function_app_name: str) -> None:
        try:
            function_app_data = await self.download_function_app_data(function_app_name)
            await self.upload_function_app_data(
                self.get_full_function_app_name(function_app_name), function_app_data, False
            )
        except RetryError:
            log.error(f"Failed to deploy {function_app_name} task.")
            return
        self.manifest_cache[function_app_name] = self.public_manifest[function_app_name]

    async def create_function_app_first_time(self, function_app_name: str) -> None:
        service_plan = await wait_for_resource(
            *await self.create_log_forwarder_app_service_plan(self.region, self.generate_app_service_plan_name())
        )
        await wait_for_resource(
            *await self.create_log_forwarder_function_app(self.region, function_app_name, service_plan.id)  # type: ignore
        )

    def get_full_function_app_name(self, func_app_name_short: str) -> str:
        return func_app_name_short + self.uuid_parts[0] + self.uuid_parts[1]

    def generate_app_service_plan_name(self) -> str:
        return f"dd-forwarder{self.uuid_parts[0]}{self.uuid_parts[1]}"

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
                        NameValuePair(name="FUNCTIONS_WORKER_RUNTIME", value="custom"),
                        NameValuePair(name="AzureWebJobsStorage", value=connection_string),
                        NameValuePair(name="FUNCTIONS_EXTENSION_VERSION", value="~4"),
                    ]
                ),
            ),
        ), lambda: self.web_client.web_apps.get(self.resource_group, function_app_name)

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def upload_function_app_data(self, function_app_name: str, function_app_data: bytes, has_run: bool) -> None:
        try:
            resp = await self.rest_client.post(
                f"https://{function_app_name}.scm.azurewebsites.net/api/publish?type=zip",  # TODO [AZINTS-2705] Figure out how to get the right name here
                data=function_app_data,
            )
            resp.raise_for_status()
        except Exception as exc:
            if not has_run:
                await self.create_function_app_first_time(function_app_name)
                await self.upload_function_app_data(function_app_name, function_app_data, True)
            else:
                raise exc

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def download_function_app_data(self, function_app_name: str) -> bytes:
        blob_name = function_app_name + "_task.zip"
        stream = await self.public_client.download_blob(blob_name)
        app_data = await stream.content_as_bytes(max_concurrency=4)
        return app_data

    async def write_caches(self) -> None:
        if self.manifest_cache == self.original_manifest_cache:
            return
        await write_cache(MANIFEST_CACHE_NAME, dumps(self.manifest_cache))


async def main():
    async with DeployerTask() as deployer:
        await deployer.run()


if __name__ == "__main__":
    basicConfig(level=INFO)
    run(main())
