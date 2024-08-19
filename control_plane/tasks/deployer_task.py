# stdlib

from asyncio import gather, run
from copy import deepcopy
from json import dumps
from logging import DEBUG, INFO, basicConfig, getLogger
from types import TracebackType
from typing import Self

# 3p
from aiohttp import ClientSession
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob.aio import ContainerClient
from tenacity import RetryError, retry, stop_after_attempt

# project
from cache.common import read_cache, write_cache
from cache.manifest_cache import MANIFEST_CACHE_NAME, ManifestCache, deserialize_manifest_cache
from tasks.task import Task

DEPLOYER_TASK_NAME = "deployer_task"

DEPLOYER_NAME = "control_plane_deployer"
MAX_ATTEMPTS = 5

PUBLIC_CONTAINER_URL = "google.com"

log = getLogger(DEPLOYER_NAME)
log.setLevel(DEBUG)


class DeployerTask(Task):
    def __init__(self) -> None:
        self.manifest_cache: ManifestCache = {}
        self.original_manifest_cache: ManifestCache = {}
        self.public_manifest: ManifestCache = {}
        self.public_client = ContainerClient.from_container_url(PUBLIC_CONTAINER_URL)
        self.rest_client = ClientSession()
        super().__init__()
        return

    async def __aenter__(self) -> Self:
        await super().__aenter__()
        await self.public_client.__aenter__()
        await self.rest_client.__aenter__()
        token = await self.credential.get_token("https://management.azure.com/.default")
        self.rest_client.headers["Authorization"] = f"Bearer {token.token}"
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        await self.public_client.__aexit__(exc_type, exc_val, exc_tb)
        await self.rest_client.__aexit__(exc_type, exc_val, exc_tb)
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
        return

    async def deploy_function_apps(self, function_app_names: list[str]) -> None:
        if len(function_app_names) == 0:
            return
        await gather(*[self.deploy_function_app(function_app_name) for function_app_name in function_app_names])
        return

    async def deploy_container_apps(self, container_app_names: list[str]) -> None:
        if len(container_app_names) == 0:
            return
        for container_app in container_app_names:
            self.manifest_cache[container_app] = self.public_manifest[container_app]
        return

    async def deploy_function_app(self, function_app_name: str) -> None:
        try:
            function_app_data = await self.download_function_app_data(function_app_name)
            await self.upload_function_app_data(function_app_name, function_app_data)
        except RetryError:
            log.error(f"Failed to deploy {function_app_name} task.")
            return
        self.manifest_cache[function_app_name] = self.public_manifest[function_app_name]

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def upload_function_app_data(self, function_app_name: str, function_app_data: bytes) -> None:
        resp = await self.rest_client.post(
            f"https://{function_app_name}.scm.azurewebsites.net/api/publish?type=zip",  # TODO [AZINTS-2705] Figure out how to get the right name here
            data=function_app_data,
        )
        resp.raise_for_status()
        return

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
        return


async def main():
    async with DeployerTask() as deployer:
        await deployer.run()


if __name__ == "__main__":
    basicConfig(level=INFO)
    run(main())
