# stdlib

from asyncio import gather, run
from logging import DEBUG, INFO, basicConfig, getLogger
from typing import Self

# 3p
from tenacity import RetryError, retry, stop_after_attempt

# project
from cache.manifest_cache import ManifestCache
from tasks.task import Task

DEPLOYER_NAME = "control_plane_deployer"
MAX_ATTEMPTS = 5

log = getLogger(DEPLOYER_NAME)
log.setLevel(DEBUG)


class Deployer(Task):
    def __init__(self) -> None:
        return None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        pass

    async def run(self) -> None:
        public_manifest: dict[str, str] = {}
        private_manifest: dict[str, str] = {}
        try:
            public_manifest, private_manifest = await gather(self.get_public_manifests(), self.get_private_manifests())
        except RetryError:
            log.error("Failed to read public manifests, exiting...")
            return
        log.info(public_manifest)
        log.info(private_manifest)
        if len(public_manifest) == 0:
            log.error("Failed to read public manifests, exiting...")
            return
        try:
            if len(private_manifest) == 0:
                await self.deploy_components(list(public_manifest.keys()))
                pass  # deploy all
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
        return {"test": "hi"}

    async def get_private_manifests(self) -> ManifestCache:
        return {"test": "hi2"}

    @retry(stop=stop_after_attempt(MAX_ATTEMPTS))
    async def deploy_components(self, component_names: list[str]) -> None:
        log.info(component_names)
        pass

    async def write_caches(self) -> None:
        pass


async def main():
    async with Deployer() as deployer:
        await deployer.run()


if __name__ == "__main__":
    basicConfig(level=INFO)
    run(main())
