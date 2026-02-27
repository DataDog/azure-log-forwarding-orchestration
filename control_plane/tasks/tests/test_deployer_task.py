# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from json import dumps
from unittest.mock import DEFAULT, AsyncMock, MagicMock, call

# 3p
from azure.core.exceptions import HttpResponseError

# project
from cache.common import InvalidCacheError
from cache.env import (
    CONTROL_PLANE_ID_SETTING,
    CONTROL_PLANE_REGION_SETTING,
    RESOURCE_GROUP_SETTING,
    STORAGE_CONNECTION_SETTING,
    SUBSCRIPTION_ID_SETTING,
)
from cache.manifest_cache import MANIFEST_CACHE_NAME, ManifestCache, deserialize_manifest_cache
from tasks.deployer_task import DEPLOYER_TASK_NAME, DeployerTask
from tasks.tests.common import AsyncMockClient, TaskTestCase, async_generator, mock
from tasks.version import VERSION

ALL_FUNCTIONS = [
    "resources-task-0863329b4b49",
    "scaling-task-0863329b4b49",
    "diagnostic-settings-task-0863329b4b49",
]


class TestDeployerTask(TaskTestCase):
    TASK_NAME = DEPLOYER_TASK_NAME

    @property
    def cache(self) -> ManifestCache:
        return self.cache_value(MANIFEST_CACHE_NAME, deserialize_manifest_cache)

    def setUp(self) -> None:
        super().setUp()
        self.env.update(
            {
                RESOURCE_GROUP_SETTING: "test_rg",
                CONTROL_PLANE_REGION_SETTING: "region1",
                SUBSCRIPTION_ID_SETTING: "0863329b-6e5c-4b49-bb0e-c87fdab76bb2",
            }
        )
        self.patch("get_config_option").side_effect = lambda k: self.env[k]
        self.patch("environ.get").side_effect = lambda k, default="unset test env var": self.env.get(k, default)
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

    def set_caches(self, public_cache: ManifestCache, private_cache: ManifestCache):
        self.public_client.download_blob.return_value.readall.return_value = dumps(public_cache).encode()
        self.read_private_cache.return_value = dumps(private_cache)

    def set_current_function_apps(self, function_apps: list[str]):
        self.web_client.web_apps.list_by_resource_group.return_value = async_generator(
            *(mock(name=app) for app in function_apps)
        )

    async def run_deployer_task(self) -> DeployerTask:
        async with DeployerTask(is_initial_run=False) as task:
            await task.run()
        return task

    async def test_deploy_task_no_diff(self):
        self.set_caches(
            public_cache={
                "resources": "1",
                "scaling": "1",
                "diagnostic_settings": "1",
            },
            private_cache={
                "resources": "1",
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
            "scaling": "1",
            "diagnostic_settings": "1",
        }
        self.set_caches(
            public_cache=public_cache,
            private_cache={
                "resources": "1",
                "scaling": "1",
                "diagnostic_settings": "1",
            },
        )
        self.set_current_function_apps(ALL_FUNCTIONS)

        await self.run_deployer_task()

        self.assertEqual(self.cache, public_cache)
        self.assertEqual(
            self.rest_client.post.mock_calls[0][1][0],
            "https://resources-task-0863329b4b49.scm.azurewebsites.net/api/zipdeploy",
        )

    async def test_deploy_task_diff_func_and_container_app(self):
        public_cache: ManifestCache = {
            "resources": "2",
            "scaling": "1",
            "diagnostic_settings": "1",
        }
        self.set_caches(
            public_cache=public_cache,
            private_cache={
                "resources": "1",
                "scaling": "1",
                "diagnostic_settings": "1",
            },
        )
        self.set_current_function_apps(ALL_FUNCTIONS)

        await self.run_deployer_task()

        self.assertEqual(self.cache, public_cache)
        self.assertEqual(
            self.rest_client.post.mock_calls[0][1][0],
            "https://resources-task-0863329b4b49.scm.azurewebsites.net/api/zipdeploy",
        )

    async def test_partial_success_func_app(self):
        self.set_caches(
            public_cache={
                "resources": "2",
                "scaling": "1",
                "diagnostic_settings": "2",
            },
            private_cache={
                "resources": "1",
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

        self.assertEqual(
            self.rest_client.post.mock_calls[0][1][0],
            "https://resources-task-0863329b4b49.scm.azurewebsites.net/api/zipdeploy",
        )
        self.assertEqual(
            self.cache,
            {
                "resources": "2",
                "scaling": "1",
                "diagnostic_settings": "1",
            },
        )
        self.public_client.download_blob.assert_has_calls(
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
            any_order=True,
        )

    async def test_deploy_task_no_public_manifest(self):
        self.public_client.download_blob.return_value.readall.return_value = b"invalid"
        self.read_private_cache.return_value = dumps(
            {
                "resources": "1",
                "diagnostic_settings": "1",
                "scaling": "1",
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
        self.set_current_function_apps(ALL_FUNCTIONS)

        await self.run_deployer_task()

        self.write_cache.assert_not_awaited()
        self.assertEqual(self.rest_client.post.await_count, 5)

    async def test_deploy_task_govcloud(self):
        self.env[CONTROL_PLANE_REGION_SETTING] = "usgovarizona"
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
        self.set_current_function_apps(ALL_FUNCTIONS)

        await self.run_deployer_task()

        self.credential.get_token.assert_awaited_once_with("https://management.usgovcloudapi.net/.default")

        self.assertEqual(
            self.cache,
            {
                "resources": "2",
                "scaling": "1",
                "diagnostic_settings": "1",
            },
        )
        self.assertEqual(
            self.rest_client.post.mock_calls[0][1][0],
            "https://resources-task-0863329b4b49.scm.azurewebsites.us/api/zipdeploy",
        )

    async def test_deployer_tags(self):
        self.env[CONTROL_PLANE_ID_SETTING] = "a2b4c5d6"
        public_cache: ManifestCache = {
            "resources": "1",
            "scaling": "1",
            "diagnostic_settings": "1",
        }
        self.set_caches(
            public_cache=public_cache,
            private_cache={
                "resources": "1",
                "scaling": "3",
                "diagnostic_settings": "4",
            },
        )

        task = await self.run_deployer_task()

        self.assertCountEqual(
            task.tags,
            [
                "forwarder:lfocontrolplane",
                "task:deployer_task",
                "control_plane_id:a2b4c5d6",
                f"version:{VERSION}",
            ],
        )


class TestFixContentShares(TaskTestCase):
    TASK_NAME = DEPLOYER_TASK_NAME

    @property
    def cache(self) -> ManifestCache:
        return self.cache_value(MANIFEST_CACHE_NAME, deserialize_manifest_cache)

    def setUp(self) -> None:
        super().setUp()
        self.env.update(
            {
                RESOURCE_GROUP_SETTING: "test_rg",
                CONTROL_PLANE_REGION_SETTING: "region1",
                SUBSCRIPTION_ID_SETTING: "0863329b-6e5c-4b49-bb0e-c87fdab76bb2",
                STORAGE_CONNECTION_SETTING: "DefaultEndpointsProtocol=https;AccountName=teststorage;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            }
        )
        self.patch("get_config_option").side_effect = lambda k: self.env[k]
        self.patch("environ.get").side_effect = lambda k, default="unset test env var": self.env.get(k, default)
        self.public_client = AsyncMockClient()
        self.patch("ContainerClient").return_value = self.public_client
        self.read_private_cache = self.patch("read_cache")
        self.rest_client = AsyncMockClient()
        self.rest_client.post.return_value = MagicMock()
        self.patch("ClientSession").return_value = self.rest_client
        self.web_client = AsyncMockClient()
        self.web_client.web_apps.list_by_resource_group = MagicMock(return_value=async_generator())
        self.web_client.web_apps.list_application_settings = AsyncMock()
        self.web_client.web_apps.update_application_settings = AsyncMock()
        self.patch("WebSiteManagementClient").return_value = self.web_client
        self.share_service = AsyncMockClient()
        self.patch("ShareServiceClient").from_connection_string.return_value = self.share_service

    def set_caches(self, public_cache: ManifestCache, private_cache: ManifestCache):
        self.public_client.download_blob.return_value.readall.return_value = dumps(public_cache).encode()
        self.read_private_cache.return_value = dumps(private_cache)

    def set_current_function_apps(self, function_apps: list[str]):
        self.web_client.web_apps.list_by_resource_group.return_value = async_generator(
            *(mock(name=app) for app in function_apps)
        )

    async def run_deployer_task(self) -> DeployerTask:
        async with DeployerTask(is_initial_run=False) as task:
            await task.run()
        return task

    def _make_settings(self, content_share_value: str) -> MagicMock:
        settings = MagicMock()
        settings.properties = {"WEBSITE_CONTENTSHARE": content_share_value, "OTHER_SETTING": "value"}
        return settings

    async def test_fix_content_shares_creates_shares(self):
        no_diff_cache: ManifestCache = {
            "resources": "1",
            "scaling": "1",
            "diagnostic_settings": "1",
        }
        self.set_caches(public_cache=no_diff_cache, private_cache=no_diff_cache)
        self.set_current_function_apps(ALL_FUNCTIONS)
        self.web_client.web_apps.list_application_settings.side_effect = [
            self._make_settings("diagnostic-settings-task-0863329b4b49"),
            self._make_settings("scaling-task-0863329b4b49"),
        ]

        await self.run_deployer_task()

        self.share_service.create_share.assert_any_await("diagnostic-settings-task-0863329b4b49")
        self.share_service.create_share.assert_any_await("scaling-task-0863329b4b49")

    async def test_fix_content_shares_updates_mismatched_settings(self):
        no_diff_cache: ManifestCache = {
            "resources": "1",
            "scaling": "1",
            "diagnostic_settings": "1",
        }
        self.set_caches(public_cache=no_diff_cache, private_cache=no_diff_cache)
        self.set_current_function_apps(ALL_FUNCTIONS)
        wrong_settings = self._make_settings("resources-task-0863329b4b49")
        self.web_client.web_apps.list_application_settings.side_effect = [
            wrong_settings,
            self._make_settings("scaling-task-0863329b4b49"),
        ]

        await self.run_deployer_task()

        self.web_client.web_apps.update_application_settings.assert_awaited_once_with(
            "test_rg", "diagnostic-settings-task-0863329b4b49", wrong_settings
        )
        self.assertEqual(wrong_settings.properties["WEBSITE_CONTENTSHARE"], "diagnostic-settings-task-0863329b4b49")

    async def test_fix_content_shares_skips_when_already_correct(self):
        no_diff_cache: ManifestCache = {
            "resources": "1",
            "scaling": "1",
            "diagnostic_settings": "1",
        }
        self.set_caches(public_cache=no_diff_cache, private_cache=no_diff_cache)
        self.set_current_function_apps(ALL_FUNCTIONS)
        self.web_client.web_apps.list_application_settings.side_effect = [
            self._make_settings("diagnostic-settings-task-0863329b4b49"),
            self._make_settings("scaling-task-0863329b4b49"),
        ]

        await self.run_deployer_task()

        self.web_client.web_apps.update_application_settings.assert_not_awaited()

    async def test_fix_content_shares_handles_existing_share(self):
        no_diff_cache: ManifestCache = {
            "resources": "1",
            "scaling": "1",
            "diagnostic_settings": "1",
        }
        self.set_caches(public_cache=no_diff_cache, private_cache=no_diff_cache)
        self.set_current_function_apps(ALL_FUNCTIONS)
        self.web_client.web_apps.list_application_settings.side_effect = [
            self._make_settings("diagnostic-settings-task-0863329b4b49"),
            self._make_settings("scaling-task-0863329b4b49"),
        ]

        error = HttpResponseError()
        error.status_code = 409
        self.share_service.create_share.side_effect = error

        await self.run_deployer_task()

        # Should not raise, and should still check app settings
        self.assertEqual(self.web_client.web_apps.list_application_settings.await_count, 2)

    async def test_fix_content_shares_skips_settings_update_on_share_creation_failure(self):
        no_diff_cache: ManifestCache = {
            "resources": "1",
            "scaling": "1",
            "diagnostic_settings": "1",
        }
        self.set_caches(public_cache=no_diff_cache, private_cache=no_diff_cache)
        self.set_current_function_apps(ALL_FUNCTIONS)

        error = HttpResponseError()
        error.status_code = 500
        self.share_service.create_share.side_effect = error

        await self.run_deployer_task()

        # Share creation failed for both apps, so settings should not be read or updated
        self.web_client.web_apps.list_application_settings.assert_not_awaited()
        self.web_client.web_apps.update_application_settings.assert_not_awaited()
