# stdlib
from json import dumps
from unittest.mock import DEFAULT, MagicMock, Mock, call

# 3p
from azure.core.exceptions import HttpResponseError

# project
from cache.common import InvalidCacheError
from cache.manifest_cache import ManifestCache
from tasks.deployer_task import DEPLOYER_TASK_NAME, DeployerTask
from tasks.tests.common import AsyncMockClient, TaskTestCase, async_generator

ALL_FUNCTIONS = [
    "resources-task-0863329b4b49",
    "scaling-task-0863329b4b49",
    "diagnostic-settings-task-0863329b4b49",
]


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

    def set_caches(self, public_cache: ManifestCache, private_cache: ManifestCache):
        self.public_client.download_blob.return_value.readall.return_value = dumps(public_cache).encode()
        self.read_private_cache.return_value = dumps(private_cache)

    def set_current_function_apps(self, function_apps: list[str]):
        self.web_client.web_apps.list_by_resource_group.return_value = async_generator(
            *(Mock(name=app) for app in function_apps)
        )

    async def run_deployer_task(self):
        async with DeployerTask() as task:
            await task.run()

    async def test_deploy_task_no_diff(self):
        self.set_caches(
            public_cache={
                "resources": "1",
                "forwarder": "1",
                "scaling": "1",
                "diagnostic_settings": "1",
            },
            private_cache={
                "resources": "1",
                "forwarder": "1",
                "scaling": "1",
                "diagnostic_settings": "1",
            },
        )
        self.set_current_function_apps(ALL_FUNCTIONS)

        await self.run_deployer_task()

        self.write_cache.assert_not_awaited()

    async def test_deploy_task_diff_func_app(self):
        public_cache: ManifestCache = {
            "resources": "2",
            "forwarder": "1",
            "scaling": "1",
            "diagnostic_settings": "1",
        }
        self.set_caches(
            public_cache=public_cache,
            private_cache={
                "resources": "1",
                "forwarder": "1",
                "scaling": "1",
                "diagnostic_settings": "1",
            },
        )
        self.set_current_function_apps(ALL_FUNCTIONS)

        await self.run_deployer_task()

        public_cache_str = dumps(public_cache)

        self.write_cache.assert_awaited_once_with("manifest.json", public_cache_str)

    async def test_deploy_task_diff_container_app(self):
        public_cache: ManifestCache = {
            "resources": "1",
            "forwarder": "2",
            "scaling": "1",
            "diagnostic_settings": "1",
        }
        self.set_caches(
            public_cache=public_cache,
            private_cache={
                "resources": "1",
                "forwarder": "1",
                "scaling": "1",
                "diagnostic_settings": "1",
            },
        )
        self.set_current_function_apps(ALL_FUNCTIONS)

        await self.run_deployer_task()

        public_cache_str = dumps(public_cache)

        self.write_cache.assert_awaited_once_with("manifest.json", public_cache_str)

    async def test_deploy_task_diff_func_and_container_app(self):
        public_cache: ManifestCache = {
            "resources": "2",
            "forwarder": "2",
            "scaling": "1",
            "diagnostic_settings": "1",
        }
        self.set_caches(
            public_cache=public_cache,
            private_cache={
                "resources": "1",
                "forwarder": "1",
                "scaling": "1",
                "diagnostic_settings": "1",
            },
        )
        self.set_current_function_apps(ALL_FUNCTIONS)

        await self.run_deployer_task()

        public_cache_str = dumps(public_cache)

        self.write_cache.assert_awaited_once_with("manifest.json", public_cache_str)

    async def test_partial_success_func_app(self):
        self.set_caches(
            public_cache={
                "resources": "2",
                "forwarder": "1",
                "scaling": "1",
                "diagnostic_settings": "2",
            },
            private_cache={
                "resources": "1",
                "forwarder": "1",
                "scaling": "1",
                "diagnostic_settings": "1",
            },
        )
        self.set_current_function_apps(ALL_FUNCTIONS)

        def _download_blob(item: str):
            if item == "diagnostic_settings_task.zip":
                raise HttpResponseError()
            return DEFAULT

        self.public_client.download_blob.side_effect = _download_blob

        await self.run_deployer_task()

        public_cache_str = dumps(
            {
                "resources": "2",
                "forwarder": "1",
                "scaling": "1",
                "diagnostic_settings": "1",
            }
        )

        self.write_cache.assert_awaited_once_with("manifest.json", public_cache_str)
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
                "forwarder": "1",
                "scaling": "1",
                "diagnostic_settings": "1",
            }
        )
        self.set_current_function_apps(ALL_FUNCTIONS)

        with self.assertRaises(InvalidCacheError) as ctx:
            await self.run_deployer_task()

        self.write_cache.assert_not_awaited()
        self.public_client.download_blob.assert_awaited_once_with("manifest.json")
        self.assertEqual("Invalid Public Manifest: invalid", str(ctx.exception))

    async def test_deploy_task_no_private_manifests(self):
        public_cache: ManifestCache = {
            "forwarder": "2",
            "resources": "2",
            "scaling": "1",
            "diagnostic_settings": "1",
        }
        self.public_client.download_blob.return_value.readall.return_value = dumps(public_cache).encode()
        self.read_private_cache.return_value = ""
        self.set_current_function_apps(ALL_FUNCTIONS)

        await self.run_deployer_task()

        public_cache_str = dumps(public_cache)

        self.write_cache.assert_awaited_once_with("manifest.json", public_cache_str)

    async def test_deploy_task_no_manifests(self):
        self.public_client.download_blob.return_value.readall.return_value = b""
        self.read_private_cache.return_value = ""
        self.set_current_function_apps(ALL_FUNCTIONS)

        with self.assertRaises(InvalidCacheError) as ctx:
            await self.run_deployer_task()

        self.write_cache.assert_not_awaited()
        self.public_client.download_blob.assert_awaited_once_with("manifest.json")
        self.assertEqual("Invalid Public Manifest: ", str(ctx.exception))

    async def test_deploy_task_private_manifest_retry_error(self):
        public_cache: ManifestCache = {
            "forwarder": "2",
            "resources": "2",
            "scaling": "1",
            "diagnostic_settings": "1",
        }
        self.public_client.download_blob.return_value.readall.return_value = dumps(public_cache).encode()
        self.read_private_cache.side_effect = HttpResponseError
        self.set_current_function_apps(ALL_FUNCTIONS)

        await self.run_deployer_task()

        public_cache_str = dumps(public_cache)

        self.assertEqual(self.read_private_cache.await_count, 5)
        self.write_cache.assert_awaited_once_with("manifest.json", public_cache_str)

    async def test_post_func_app_fails(self):
        self.set_caches(
            public_cache={
                "resources": "2",
                "forwarder": "1",
                "scaling": "1",
                "diagnostic_settings": "1",
            },
            private_cache={
                "resources": "1",
                "forwarder": "1",
                "scaling": "1",
                "diagnostic_settings": "1",
            },
        )
        self.rest_client.post.return_value.ok = False
        self.set_current_function_apps(ALL_FUNCTIONS)

        await self.run_deployer_task()

        self.write_cache.assert_not_awaited()
        self.assertEqual(self.rest_client.post.await_count, 5)
