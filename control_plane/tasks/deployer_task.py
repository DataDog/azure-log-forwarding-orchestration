# stdlib

from asyncio import gather, run
from copy import deepcopy
from json import dumps
from logging import DEBUG, INFO, basicConfig, getLogger
from types import TracebackType
from typing import Self

# 3p
from aiohttp import ClientSession
from azure.storage.blob.aio import ContainerClient
from tenacity import RetryError, retry, stop_after_attempt

# project
from cache.common import read_cache, write_cache
from cache.manifest_cache import MANIFEST_CACHE_NAME, ManifestCache, deserialize_manifest_cache
from tasks.task import Task

DEPLOYER_NAME = "control_plane_deployer"
MAX_ATTEMPTS = 5

PUBLIC_CONTAINER_URL = "google.com"

log = getLogger(DEPLOYER_NAME)
log.setLevel(DEBUG)


class Deployer(Task):
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
        await super().__aexit__()

    async def run(self) -> None:
        public_manifest: dict[str, str] = {}
        private_manifest: dict[str, str] = {}
        try:
            public_manifest, private_manifest = await gather(self.get_public_manifests(), self.get_private_manifests())
        except RetryError:
            log.error("Failed to read public manifests, exiting...")
            return
        if len(public_manifest) == 0:
            log.error("Failed to read public manifests, exiting...")
            return
        self.manifest_cache = deepcopy(private_manifest)
        self.original_manifest_cache = private_manifest
        self.public_manifest = public_manifest
        try:
            if len(private_manifest) == 0:
                await self.deploy_components(list(public_manifest.keys()))  # deploy all
            else:
                await self.deploy_components(
                    [
                        component
                        for component in public_manifest
                        if public_manifest[component] != private_manifest[component]
                    ]
                )
                pass
        except RetryError:
            log.error("Failed to successfully deploy, exiting...")
            return
        return

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def get_public_manifests(self) -> ManifestCache:
        stream = await self.public_client.download_blob(MANIFEST_CACHE_NAME)
        blob_data = await stream.content_as_bytes(max_concurrency=4)
        validated_blob = deserialize_manifest_cache(blob_data.decode())
        if validated_blob:
            return validated_blob
        return {}

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def get_private_manifests(self) -> ManifestCache:
        blob_data = await read_cache(MANIFEST_CACHE_NAME)
        validated_blob = deserialize_manifest_cache(blob_data)
        if validated_blob:
            return validated_blob
        return {}

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def deploy_components(self, component_names: list[str]) -> None:
        log.info(component_names)
        pass

    async def deploy_function_apps(self, function_app_names: list[str]) -> None:
        try:
            await gather(*[self.deploy_function_app(function_app_name) for function_app_name in function_app_names])
        except RetryError:
            log.error("Error deploy function apps")
        return

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def deploy_container_apps(self, container_app_names: list[str]) -> None:
        pass

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def deploy_function_app(self, function_app_name: str) -> None:
        function_app_data = await self.download_function_app_data(function_app_name)
        resp = await self.rest_client.post(
            f"https://{function_app_name}.scm.azurewebsites.net/api/publish?type=zip",  # TODO ASK HERE
            data=function_app_data,
        )
        resp.raise_for_status()
        self.manifest_cache[function_app_name] = self.public_manifest[function_app_name]

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def download_function_app_data(self, function_app_name: str):
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
    async with Deployer() as deployer:
        await deployer.run()


if __name__ == "__main__":
    basicConfig(level=INFO)
    run(main())
