# stdlib
from asyncio import run
from json import dumps
from unittest.mock import DEFAULT, AsyncMock, MagicMock

# 3p
from aiohttp import ClientResponseError
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from tenacity import RetryError

# project
from cache.manifest_cache import ManifestCache
from tasks.deployer_task import DEPLOYER_TASK_NAME, DeployerTask
from tasks.tests.common import TaskTestCase


class TestDeployerTask(TaskTestCase):
    TASK_NAME = DEPLOYER_TASK_NAME

    def setUp(self) -> None:
        super().setUp()
        container_client = self.patch("ContainerClient")
        client_session = self.patch("ClientSession")
        client_session.return_value = AsyncMock()
        container_client.from_container_url.return_value = AsyncMock()
        self.public_client: AsyncMock = container_client.from_container_url.return_value
        self.rest_client: AsyncMock = client_session.return_value

        self.log = self.patch("log")

    async def run_resources_task(self):
        async with DeployerTask() as task:
            await task.run()

    async def test_deploy_task_no_diff(self):
        get_public_manifests: AsyncMock = self.patch("DeployerTask.get_public_manifests")
        get_private_manifests: AsyncMock = self.patch("DeployerTask.get_private_manifests")

        public_cache: ManifestCache = {
            "resources": "1",
            "forwarder": "1",
            "diagnostic_settings": "1",
            "scaling": "1",
        }
        private_cache: ManifestCache = {
            "resources": "1",
            "forwarder": "1",
            "diagnostic_settings": "1",
            "scaling": "1",
        }

        get_public_manifests.return_value = public_cache
        get_private_manifests.return_value = private_cache

        await self.run_resources_task()

        self.write_cache.assert_not_awaited()
        self.log.error.assert_not_called()

    async def test_deploy_task_diff_func_app(self):
        get_public_manifests: AsyncMock = self.patch("DeployerTask.get_public_manifests")
        get_private_manifests: AsyncMock = self.patch("DeployerTask.get_private_manifests")

        public_cache: ManifestCache = {
            "resources": "2",
            "forwarder": "1",
            "diagnostic_settings": "1",
            "scaling": "1",
        }
        private_cache: ManifestCache = {
            "resources": "1",
            "forwarder": "1",
            "diagnostic_settings": "1",
            "scaling": "1",
        }

        get_public_manifests.return_value = public_cache
        get_private_manifests.return_value = private_cache

        self.public_client.download_blob.return_value = AsyncMock()
        self.public_client.download_blob.return_value.content_as_bytes.return_value = bytes("test", "utf-8")

        self.rest_client.post.return_value = MagicMock()

        await self.run_resources_task()

        public_cache_str = dumps(public_cache)

        self.write_cache.assert_awaited_once_with("manifest.json", public_cache_str)
        self.log.error.assert_not_called()

    async def test_deploy_task_diff_container_app(self):
        get_public_manifests: AsyncMock = self.patch("DeployerTask.get_public_manifests")
        get_private_manifests: AsyncMock = self.patch("DeployerTask.get_private_manifests")

        public_cache: ManifestCache = {
            "resources": "1",
            "forwarder": "2",
            "diagnostic_settings": "1",
            "scaling": "1",
        }
        private_cache: ManifestCache = {
            "resources": "1",
            "forwarder": "1",
            "diagnostic_settings": "1",
            "scaling": "1",
        }

        get_public_manifests.return_value = public_cache
        get_private_manifests.return_value = private_cache

        await self.run_resources_task()

        public_cache_str = dumps(public_cache)

        self.write_cache.assert_awaited_once_with("manifest.json", public_cache_str)
        self.log.error.assert_not_called()

    async def test_deploy_task_diff_func_and_container_app(self):
        get_public_manifests: AsyncMock = self.patch("DeployerTask.get_public_manifests")
        get_private_manifests: AsyncMock = self.patch("DeployerTask.get_private_manifests")

        public_cache: ManifestCache = {
            "resources": "2",
            "forwarder": "2",
            "diagnostic_settings": "1",
            "scaling": "1",
        }
        private_cache: ManifestCache = {
            "resources": "1",
            "forwarder": "1",
            "diagnostic_settings": "1",
            "scaling": "1",
        }

        get_public_manifests.return_value = public_cache
        get_private_manifests.return_value = private_cache

        self.public_client.download_blob.return_value = AsyncMock()
        self.public_client.download_blob.return_value.content_as_bytes.return_value = bytes("test", "utf-8")

        self.rest_client.post.return_value = MagicMock()

        await self.run_resources_task()

        public_cache_str = dumps(public_cache)

        self.write_cache.assert_awaited_once_with("manifest.json", public_cache_str)
        self.log.error.assert_not_called()

    async def test_partial_success_func_app(self):
        get_public_manifests: AsyncMock = self.patch("DeployerTask.get_public_manifests")
        get_private_manifests: AsyncMock = self.patch("DeployerTask.get_private_manifests")

        public_cache: ManifestCache = {
            "resources": "2",
            "forwarder": "1",
            "diagnostic_settings": "2",
            "scaling": "1",
        }
        private_cache: ManifestCache = {
            "resources": "1",
            "forwarder": "1",
            "diagnostic_settings": "1",
            "scaling": "1",
        }

        get_public_manifests.return_value = public_cache
        get_private_manifests.return_value = private_cache

        self.public_client.download_blob.return_value = AsyncMock()
        self.public_client.download_blob.side_effect = [
            DEFAULT,
            HttpResponseError,
            HttpResponseError,
            HttpResponseError,
            HttpResponseError,
            HttpResponseError,
        ]
        self.public_client.download_blob.return_value.content_as_bytes.return_value = bytes("test", "utf-8")

        self.rest_client.post.return_value = MagicMock()

        await self.run_resources_task()

        public_cache_str = dumps(
            {
                "resources": "2",
                "forwarder": "1",
                "diagnostic_settings": "1",
                "scaling": "1",
            }
        )

        self.write_cache.assert_awaited_once_with("manifest.json", public_cache_str)
        self.assertEqual(self.public_client.download_blob.await_count, 6)
        self.log.error.assert_called_with("Failed to deploy diagnostic_settings task.")

    async def test_deploy_task_no_public_manifest(self):
        get_public_manifests: AsyncMock = self.patch("DeployerTask.get_public_manifests")
        get_private_manifests: AsyncMock = self.patch("DeployerTask.get_private_manifests")

        public_cache: ManifestCache = {}
        private_cache: ManifestCache = {
            "resources": "1",
            "forwarder": "1",
            "diagnostic_settings": "1",
            "scaling": "1",
        }

        get_public_manifests.return_value = public_cache
        get_private_manifests.return_value = private_cache

        self.public_client.download_blob.return_value = AsyncMock()
        self.public_client.download_blob.return_value.content_as_bytes.return_value = bytes("test", "utf-8")

        self.rest_client.post.return_value = MagicMock()

        await self.run_resources_task()

        self.write_cache.assert_not_awaited()
        self.assertEqual(self.public_client.download_blob.await_count, 0)
        self.log.error.assert_called_once_with("Failed to read public manifests, exiting...")

    async def test_deploy_task_no_private_manifests(self):
        get_public_manifests: AsyncMock = self.patch("DeployerTask.get_public_manifests")
        get_private_manifests: AsyncMock = self.patch("DeployerTask.get_private_manifests")

        public_cache: ManifestCache = {
            "forwarder": "2",
            "resources": "2",
            "diagnostic_settings": "1",
            "scaling": "1",
        }
        private_cache: ManifestCache = {}

        get_public_manifests.return_value = public_cache
        get_private_manifests.return_value = private_cache

        self.public_client.download_blob.return_value = AsyncMock()
        self.public_client.download_blob.return_value.content_as_bytes.return_value = bytes("test", "utf-8")

        self.rest_client.post.return_value = MagicMock()

        await self.run_resources_task()

        public_cache_str = dumps(public_cache)

        self.write_cache.assert_awaited_once_with("manifest.json", public_cache_str)
        self.log.error.assert_not_called()

    async def test_deploy_task_no_manifests(self):
        get_public_manifests: AsyncMock = self.patch("DeployerTask.get_public_manifests")
        get_private_manifests: AsyncMock = self.patch("DeployerTask.get_private_manifests")

        public_cache: ManifestCache = {}
        private_cache: ManifestCache = {}

        get_public_manifests.return_value = public_cache
        get_private_manifests.return_value = private_cache

        self.public_client.download_blob.return_value = AsyncMock()
        self.public_client.download_blob.return_value.content_as_bytes.return_value = bytes("test", "utf-8")

        self.rest_client.post.return_value = MagicMock()

        await self.run_resources_task()

        self.write_cache.assert_not_awaited()
        self.assertEqual(self.public_client.download_blob.await_count, 0)
        self.log.error.assert_called_once_with("Failed to read public manifests, exiting...")

    async def test_deploy_task_private_manifest_retry_error(self):
        get_public_manifests: AsyncMock = self.patch("DeployerTask.get_public_manifests")
        read_cache: AsyncMock = self.patch("read_cache")

        public_cache: ManifestCache = {
            "forwarder": "2",
            "resources": "2",
            "diagnostic_settings": "1",
            "scaling": "1",
        }

        get_public_manifests.return_value = public_cache
        read_cache.side_effect = HttpResponseError

        self.public_client.download_blob.return_value = AsyncMock()
        self.public_client.download_blob.return_value.content_as_bytes.return_value = bytes("test", "utf-8")

        self.rest_client.post.return_value = MagicMock()

        await self.run_resources_task()

        public_cache_str = dumps(public_cache)

        self.assertEqual(read_cache.await_count, 5)
        self.write_cache.assert_awaited_once_with("manifest.json", public_cache_str)
        self.log.warn.assert_called_with(
            "Failed to read private manifests. Manifests may not exist or error may have occured."
        )

    async def test_post_func_app_fails(self):
        get_public_manifests: AsyncMock = self.patch("DeployerTask.get_public_manifests")
        get_private_manifests: AsyncMock = self.patch("DeployerTask.get_private_manifests")

        public_cache: ManifestCache = {
            "resources": "2",
            "forwarder": "1",
            "diagnostic_settings": "1",
            "scaling": "1",
        }
        private_cache: ManifestCache = {
            "resources": "1",
            "forwarder": "1",
            "diagnostic_settings": "1",
            "scaling": "1",
        }

        get_public_manifests.return_value = public_cache
        get_private_manifests.return_value = private_cache

        self.public_client.download_blob.return_value = AsyncMock()
        self.public_client.download_blob.return_value.content_as_bytes.return_value = bytes("test", "utf-8")

        resp = MagicMock()
        resp.raise_for_status.side_effect = ClientResponseError
        self.rest_client.post.return_value = resp

        await self.run_resources_task()

        self.write_cache.assert_not_awaited()
        self.assertEqual(self.rest_client.post.await_count, 5)
        self.log.error.assert_called_with("Failed to deploy resources task.")

    async def public_manifest_helper(self) -> ManifestCache:
        return await DeployerTask().get_public_manifests()

    async def private_manifest_helper(self) -> ManifestCache:
        return await DeployerTask().get_private_manifests()

    def test_get_public_manifest_normal(self):
        self.public_client.download_blob.return_value = AsyncMock()
        self.public_client.download_blob.return_value.content_as_bytes.return_value = MagicMock()
        public_cache: ManifestCache = {
            "resources": "2",
            "forwarder": "1",
            "diagnostic_settings": "1",
            "scaling": "1",
        }
        self.public_client.download_blob.return_value.content_as_bytes.return_value.decode.return_value = dumps(
            public_cache
        )
        returned_cache = run(self.public_manifest_helper())
        self.assertEqual(public_cache, returned_cache)

    def test_get_public_manifest_invalid_cache(self):
        self.public_client.download_blob.return_value = AsyncMock()
        self.public_client.download_blob.return_value.content_as_bytes.return_value = MagicMock()
        public_cache: ManifestCache = {
            "resources": "2",
            "forward": "1",
            "diagnostic_settings": "1",
            "scaling": "1",
        }
        self.public_client.download_blob.return_value.content_as_bytes.return_value.decode.return_value = dumps(
            public_cache
        )
        returned_cache = run(self.public_manifest_helper())
        self.assertEqual({}, returned_cache)

    def test_get_public_manifest_no_resource(self):
        self.public_client.download_blob.side_effect = ResourceNotFoundError
        returned_cache = run(self.public_manifest_helper())
        self.assertEqual({}, returned_cache)
        self.assertEqual(self.public_client.download_blob.await_count, 1)

    def test_get_public_manifest_http_error(self):
        self.public_client.download_blob.side_effect = HttpResponseError
        with self.assertRaises(RetryError) as ctx:
            returned_cache = run(self.public_manifest_helper())
            self.assertEqual({}, returned_cache)
            self.assertEqual(self.public_client.download_blob.await_count, 5)
        self.assertIsInstance(ctx.exception.last_attempt.exception(), HttpResponseError)

    def test_get_private_manifests_normal(self):
        read_cache: AsyncMock = self.patch("read_cache")
        private_cache: ManifestCache = {
            "resources": "1",
            "forwarder": "1",
            "diagnostic_settings": "1",
            "scaling": "1",
        }
        read_cache.return_value = dumps(private_cache)
        returned_cache = run(self.private_manifest_helper())
        self.assertEqual(returned_cache, private_cache)

    def test_get_private_manifests_invalid_cache(self):
        read_cache: AsyncMock = self.patch("read_cache")
        private_cache: ManifestCache = {
            "resources": "1",
            "forward": "1",
            "diagnostic_settings": "1",
            "scaling": "1",
        }
        read_cache.return_value = dumps(private_cache)
        returned_cache = run(self.private_manifest_helper())
        self.assertEqual(returned_cache, {})

    def test_get_private_manifests_error(self):
        read_cache: AsyncMock = self.patch("read_cache")
        read_cache.side_effect = HttpResponseError
        returned_cache = run(self.private_manifest_helper())
        self.assertEqual(returned_cache, {})
        self.assertEqual(read_cache.await_count, 5)
