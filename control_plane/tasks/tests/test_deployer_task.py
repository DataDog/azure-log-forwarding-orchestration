# stdlib
from json import dumps
from unittest.mock import DEFAULT, MagicMock, Mock, call

# 3p
from azure.core.exceptions import HttpResponseError

# project
from cache.common import InvalidCacheError
from cache.manifest_cache import ManifestCache, PublicManifest
from tasks.deployer_task import DEPLOYER_TASK_NAME, DeployerTask
from tasks.tests.common import AsyncMockClient, TaskTestCase, async_generator

ALL_FUNCTIONS = [
    "resources-task-0863329b4b49",
    "scaling-task-0863329b4b49",
    "diagnostic-settings-task-0863329b4b49",
]

OLD_VERSION = "v47707852-0fafeed8"
NEW_VERSION = "v47826247-f7abf93e"


class TestDeployerTask(TaskTestCase):
    TASK_NAME = DEPLOYER_TASK_NAME

    def setUp(self) -> None:
        super().setUp()
        self.patch("get_config_option").side_effect = {
            "RESOURCE_GROUP": "test_rg",
            "REGION": "region1",
            "SUBSCRIPTION_ID": "0863329b-6e5c-4b49-bb0e-c87fdab76bb2",
        }.get

        self.public_client = AsyncMockClient()
        self.patch("ContainerClient").return_value = self.public_client
        self.read_private_cache = self.patch("read_cache")
        self.rest_client = AsyncMockClient()
        self.rest_client.post.return_value = MagicMock()
        self.patch("ClientSession").return_value = self.rest_client
        self.web_client = AsyncMockClient()
        self.web_client.web_apps.list_by_resource_group = MagicMock(return_value=async_generator())
        self.web_client.app_service_plans.list_by_resource_group = MagicMock(return_value=async_generator())
        self.patch("WebSiteManagementClient").return_value = self.web_client

        self.storage_client = AsyncMockClient()
        self.storage_client.storage_accounts.list_by_resource_group = MagicMock(return_value=async_generator())
        self.patch("StorageManagementClient").return_value = self.storage_client

        self.public_manifest: PublicManifest = {
            "latest": {
                "resources": NEW_VERSION,
                "diagnostic_settings": NEW_VERSION,
                "scaling": NEW_VERSION,
            },
            "version_history": {
                NEW_VERSION: {
                    "resources": "19408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
                    "diagnostic_settings": "29408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
                    "scaling": "39408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
                },
                OLD_VERSION: {
                    "resources": "49408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
                    "diagnostic_settings": "59408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
                    "scaling": "69408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
                },
            },
        }

    def set_caches(self, public_cache: PublicManifest, private_cache: ManifestCache):
        self.public_client.download_blob.return_value.readall.return_value = dumps(public_cache).encode()
        self.read_private_cache.return_value = dumps(private_cache)

    def set_current_function_apps(self, function_apps: list[str] | None = None):
        """Sets up which function apps are currently deployed, defaults to all"""
        if function_apps is None:
            function_apps = ALL_FUNCTIONS
        self.web_client.web_apps.list_by_resource_group.return_value = async_generator(
            *(Mock(name=app) for app in function_apps)
        )

    async def run_deployer_task(self):
        async with DeployerTask() as task:
            await task.run()

    async def test_deploy_task_no_diff(self):
        self.set_caches(
            public_cache=self.public_manifest,
            private_cache={
                "resources": NEW_VERSION,
                "scaling": NEW_VERSION,
                "diagnostic_settings": NEW_VERSION,
            },
        )
        self.set_current_function_apps()

        await self.run_deployer_task()

        self.write_cache.assert_not_awaited()

    async def test_deploy_task_private_cache_not_latest(self):
        self.set_caches(
            public_cache=self.public_manifest,
            private_cache={
                "resources": OLD_VERSION,
                "scaling": NEW_VERSION,
                "diagnostic_settings": NEW_VERSION,
            },
        )
        self.set_current_function_apps()

        await self.run_deployer_task()

        self.write_cache.assert_awaited_once_with(
            "manifest.json",
            dumps(
                {
                    "resources": NEW_VERSION,
                    "scaling": NEW_VERSION,
                    "diagnostic_settings": NEW_VERSION,
                }
            ),
        )

    async def test_partial_success_doesnt_write_to_cache(self):
        self.set_caches(
            public_cache=self.public_manifest,
            private_cache={
                "resources": OLD_VERSION,
                "scaling": OLD_VERSION,
                "diagnostic_settings": OLD_VERSION,
            },
        )
        self.set_current_function_apps()

        def _download_blob(item: str):
            if item.startswith("diagnostic_settings_task"):
                raise HttpResponseError()
            return DEFAULT

        self.public_client.download_blob.side_effect = _download_blob

        await self.run_deployer_task()

        self.write_cache.assert_not_awaited()
        self.assertEqual(
            self.public_client.download_blob.mock_calls,
            [
                call("manifest.json"),
                call().readall(),
                call("resources_task.zip"),
                call().readall(),
                call("diagnostic_settings_task.zip"),
                call("diagnostic_settings_task.zip"),
                call("diagnostic_settings_task.zip"),
                call("diagnostic_settings_task.zip"),
                call("diagnostic_settings_task.zip"),
            ],
        )

    async def test_deploy_task_no_public_manifest(self):
        self.public_client.download_blob.return_value.readall.return_value = b"invalid"
        self.read_private_cache.return_value = dumps(
            {
                "resources": "1",
                "scaling": "1",
                "diagnostic_settings": "1",
            }
        )
        self.set_current_function_apps()

        with self.assertRaises(InvalidCacheError) as ctx:
            await self.run_deployer_task()

        self.write_cache.assert_not_awaited()
        self.public_client.download_blob.assert_awaited_once_with("manifest.json")
        self.assertEqual("Invalid Public Manifest: invalid", str(ctx.exception))

    async def test_deploy_task_no_private_manifests(self):
        self.public_client.download_blob.return_value.readall.return_value = dumps(self.public_manifest).encode()
        self.read_private_cache.return_value = ""
        self.set_current_function_apps()

        await self.run_deployer_task()

        self.write_cache.assert_awaited_once_with(
            "manifest.json",
            dumps(
                {
                    "resources": NEW_VERSION,
                    "scaling": NEW_VERSION,
                    "diagnostic_settings": NEW_VERSION,
                }
            ),
        )

    async def test_deploy_task_no_manifests(self):
        self.public_client.download_blob.return_value.readall.return_value = b""
        self.read_private_cache.return_value = ""
        self.set_current_function_apps()

        with self.assertRaises(InvalidCacheError) as ctx:
            await self.run_deployer_task()

        self.write_cache.assert_not_awaited()
        self.public_client.download_blob.assert_awaited_once_with("manifest.json")
        self.assertEqual("Invalid Public Manifest: ", str(ctx.exception))

    async def test_deploy_task_private_manifest_retry_error(self):
        public_cache: PublicManifest = {
            "resources": "2",
            "scaling": "1",
            "diagnostic_settings": "1",
        }
        self.public_client.download_blob.return_value.readall.return_value = dumps(public_cache).encode()
        self.read_private_cache.side_effect = HttpResponseError
        self.set_current_function_apps()

        await self.run_deployer_task()

        public_cache_str = dumps(public_cache)

        self.assertEqual(self.read_private_cache.await_count, 5)
        self.write_cache.assert_awaited_once_with("manifest.json", public_cache_str)

    async def test_post_func_app_fails(self):
        self.set_caches(
            public_cache={
                "resources": "2",
                "scaling": "1",
                "diagnostic_settings": "1",
            },
            private_cache={
                "resources": "1",
                "scaling": "1",
                "diagnostic_settings": "1",
            },
        )
        self.rest_client.post.return_value.ok = False
        self.set_current_function_apps()

        await self.run_deployer_task()

        self.write_cache.assert_not_awaited()
        self.assertEqual(self.rest_client.post.await_count, 5)

    async def test_deploy_hash_mismatch(self):
        pass
