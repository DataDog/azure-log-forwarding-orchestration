# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from json import dumps
from unittest.mock import AsyncMock, DEFAULT, MagicMock, call

# 3p
from azure.core.exceptions import HttpResponseError

# project
from cache.common import InvalidCacheError
from cache.env import (
    CONTROL_PLANE_ID_SETTING,
    CONTROL_PLANE_REGION_SETTING,
    RESOURCE_GROUP_SETTING,
    SUBSCRIPTION_ID_SETTING,
)
from cache.manifest_cache import MANIFEST_CACHE_NAME, ManifestCache, deserialize_manifest_cache
from tasks.deployer_task import DEPLOYER_TASK_NAME, DeployError, DeployerTask
from tasks.tests.common import AsyncMockClient, TaskTestCase, async_generator, mock
from tasks.version import VERSION

ALL_FUNCTIONS = [
    "resources-task-0863329b4b49",
    "scaling-task-0863329b4b49",
    "diagnostic-settings-task-0863329b4b49",
]

RESOURCES_STATUS_URL = "https://resources-task-0863329b4b49.scm.azurewebsites.net/api/deployments/latest"


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
        # Default: GET returns success (deployment complete), POST returns 202
        self.rest_client.get.return_value.ok = True
        self.rest_client.get.return_value.status = 200
        self.rest_client.get.return_value.json = AsyncMock(return_value={"complete": True, "status": 4})
        self.rest_client.post.return_value = MagicMock(ok=True)
        self.patch("ClientSession").return_value = self.rest_client
        self.web_client = AsyncMockClient()
        self.web_client.web_apps.list_by_resource_group = MagicMock(return_value=async_generator())
        self.web_client.app_service_plans.list_by_resource_group = MagicMock(return_value=async_generator())
        self.web_client.web_apps.list_application_settings = AsyncMock(
            side_effect=lambda rg, app_name: mock(properties={"WEBSITE_CONTENTSHARE": f"contentshare-{app_name}"})
        )
        self.web_client.web_apps.update_application_settings = AsyncMock()
        self.patch("WebSiteManagementClient").return_value = self.web_client

    def set_caches(self, public_cache: ManifestCache, private_cache: ManifestCache):
        self.public_client.download_blob.return_value.readall.return_value = dumps(public_cache).encode()
        self.read_private_cache.return_value = dumps(private_cache)

    def set_current_function_apps(self, function_apps: list[str]):
        self.web_client.web_apps.list_by_resource_group.return_value = async_generator(
            *(mock(name=app) for app in function_apps)
        )

    def set_kudu_in_progress(self):
        self.rest_client.get.return_value.json = AsyncMock(return_value={"complete": False, "status": 1})

    def set_kudu_no_deployment(self):
        self.rest_client.get.return_value.ok = False
        self.rest_client.get.return_value.status = 404

    async def run_deployer_task(self) -> DeployerTask:
        async with DeployerTask(is_initial_run=False) as task:
            await task.run()
        return task

    # -------------------------------------------------------------------------
    # Integration tests
    # -------------------------------------------------------------------------

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
        """Kudu shows success: sync triggers, update manifest."""
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
        # GET was used to check deployment status
        self.rest_client.get.assert_awaited()
        # No zip deploy POST (deployment already succeeded per Kudu)
        zip_calls = [c for c in self.rest_client.post.call_args_list if "zipdeploy" in str(c)]
        self.assertFalse(zip_calls)

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

    async def test_deploy_task_starts_async_deploy_when_no_kudu_deployment(self):
        """Kudu returns 404 (no prior deployment): start async zip deploy, don't update manifest."""
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
        self.set_kudu_no_deployment()
        self.rest_client.post.return_value = MagicMock(status=202)

        await self.run_deployer_task()

        self.assertEqual(
            self.rest_client.post.mock_calls[0][1][0],
            "https://resources-task-0863329b4b49.scm.azurewebsites.net/api/zipdeploy?isAsync=true",
        )
        self.write_cache.assert_not_awaited()

    async def test_deploy_task_skips_when_deployment_in_progress(self):
        """Kudu shows in-progress: skip, don't deploy, don't update manifest."""
        self.set_caches(
            public_cache={"resources": "2", "scaling": "1", "diagnostic_settings": "1"},
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_function_apps(ALL_FUNCTIONS)
        self.set_kudu_in_progress()

        await self.run_deployer_task()

        self.rest_client.post.assert_not_awaited()
        self.write_cache.assert_not_awaited()

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

        # resources: Kudu success → sync + update manifest
        # diagnostic_settings: Kudu 404 → attempt deploy → download fails
        def _get_side_effect(url: str, **kwargs):
            m = MagicMock()
            m.ok = True
            m.status = 200
            if "diagnostic-settings" in url:
                m.ok = False
                m.status = 404
            m.json = AsyncMock(return_value={"complete": True, "status": 4})
            m.content.read = AsyncMock(return_value=b"")
            return m

        self.rest_client.get.side_effect = _get_side_effect
        self.public_client.download_blob.side_effect = _download_blob

        await self.run_deployer_task()

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
        """Kudu 404 → attempt deploy → POST returns 400 (no retry): manifest not updated."""
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
        self.set_kudu_no_deployment()
        self.rest_client.post.return_value = MagicMock(status=400)
        self.rest_client.post.return_value.content.read = AsyncMock(return_value=b"")
        self.set_current_function_apps(ALL_FUNCTIONS)

        await self.run_deployer_task()

        self.write_cache.assert_not_awaited()
        # No retry on zip deploy
        self.assertEqual(self.rest_client.post.await_count, 1)

    async def test_deploy_task_govcloud(self):
        self.env[CONTROL_PLANE_REGION_SETTING] = "usgovarizona"
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

        self.credential.get_token.assert_awaited_once_with("https://management.usgovcloudapi.net/.default")
        self.assertEqual(self.cache, public_cache)
        # GET status was checked on the .us domain
        get_urls = [str(c) for c in self.rest_client.get.call_args_list]
        self.assertTrue(any("azurewebsites.us" in u for u in get_urls))

    async def test_fix_content_share__fixes_wrong_value(self):
        wrong_settings = mock(properties={"WEBSITE_CONTENTSHARE": "resources-task-0863329b4b49"})
        self.web_client.web_apps.list_application_settings = AsyncMock(return_value=wrong_settings)
        self.web_client.web_apps.update_application_settings = AsyncMock()
        task = DeployerTask(is_initial_run=False)

        # check that the fix is applied to the resources-task
        result = await task.fix_content_share("resources-task-0863329b4b49", "resources-task-0863329b4b49")
        self.assertTrue(result)
        self.assertEqual(wrong_settings.properties["WEBSITE_CONTENTSHARE"], "contentshare-resources-task-0863329b4b49")
        self.web_client.web_apps.update_application_settings.assert_awaited_once()

        # check that the fix is applied to the scaling-task
        wrong_scaling_settings = mock(properties={"WEBSITE_CONTENTSHARE": "resources-task-0863329b4b49"})
        self.web_client.web_apps.list_application_settings = AsyncMock(return_value=wrong_scaling_settings)
        self.web_client.web_apps.update_application_settings.reset_mock()
        result = await task.fix_content_share("scaling-task-0863329b4b49", "resources-task-0863329b4b49")
        self.assertTrue(result)
        self.assertEqual(wrong_scaling_settings.properties["WEBSITE_CONTENTSHARE"], "contentshare-scaling-task-0863329b4b49")
        self.web_client.web_apps.update_application_settings.assert_awaited_once()

    async def test_fix_content_share__fixes_unrecognized_value(self):
        unrecognized_settings = mock(properties={"WEBSITE_CONTENTSHARE": "some-other-value"})
        self.web_client.web_apps.list_application_settings = AsyncMock(return_value=unrecognized_settings)
        self.web_client.web_apps.update_application_settings = AsyncMock()

        task = DeployerTask(is_initial_run=False)
        result = await task.fix_content_share("resources-task-0863329b4b49", "resources-task-0863329b4b49")

        self.assertFalse(result)
        self.web_client.web_apps.update_application_settings.assert_not_awaited()

    async def test_fix_content_share__skips_correct_value(self):
        correct_settings = mock(properties={"WEBSITE_CONTENTSHARE": "contentshare-resources-task-0863329b4b49"})
        self.web_client.web_apps.list_application_settings = AsyncMock(return_value=correct_settings)
        self.web_client.web_apps.update_application_settings = AsyncMock()

        task = DeployerTask(is_initial_run=False)
        result = await task.fix_content_share("resources-task-0863329b4b49", "resources-task-0863329b4b49")

        self.assertFalse(result)
        self.web_client.web_apps.update_application_settings.assert_not_awaited()

    async def test_fix_content_share__skips_python_script_value(self):
        correct_settings = mock(properties={"WEBSITE_CONTENTSHARE": "resources-task-0863329b4b49123412341234"})
        self.web_client.web_apps.list_application_settings = AsyncMock(return_value=correct_settings)
        self.web_client.web_apps.update_application_settings = AsyncMock()

        task = DeployerTask(is_initial_run=False)
        result = await task.fix_content_share("resources-task-0863329b4b49", "resources-task-0863329b4b49")

        self.assertFalse(result)
        self.web_client.web_apps.update_application_settings.assert_not_awaited()

    async def test_deploy_skipped_after_content_share_fix(self):
        self.set_caches(
            public_cache={"resources": "2", "scaling": "1", "diagnostic_settings": "1"},
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_function_apps(ALL_FUNCTIONS)
        self.web_client.web_apps.list_application_settings = AsyncMock(
            return_value=mock(properties={"WEBSITE_CONTENTSHARE": "resources-task-0863329b4b49"})
        )
        self.web_client.web_apps.update_application_settings = AsyncMock()

        await self.run_deployer_task()

        self.web_client.web_apps.update_application_settings.assert_awaited()
        self.rest_client.post.assert_not_awaited()
        self.write_cache.assert_not_awaited()

    async def test_deploy_proceeds_when_content_share_is_correct_value(self):
        self.set_caches(
            public_cache={"resources": "2", "scaling": "1", "diagnostic_settings": "1"},
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_function_apps(["resources-task-0863329b4b49"])
        self.set_kudu_no_deployment()
        self.rest_client.post.return_value = MagicMock(status=202)

        def correct_settings(resource_group, function_app_name):
            return mock(properties={"WEBSITE_CONTENTSHARE": f"contentshare-{function_app_name}"})

        self.web_client.web_apps.list_application_settings = AsyncMock(side_effect=correct_settings)
        self.web_client.web_apps.update_application_settings = AsyncMock()

        await self.run_deployer_task()

        self.web_client.web_apps.update_application_settings.assert_not_awaited()
        self.assertEqual(
            self.rest_client.post.mock_calls[0][1][0],
            "https://resources-task-0863329b4b49.scm.azurewebsites.net/api/zipdeploy?isAsync=true",
        )

    async def test_deploy_proceeds_when_content_share_is_python_script_value(self):
        self.set_caches(
            public_cache={"resources": "2", "scaling": "1", "diagnostic_settings": "1"},
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_function_apps(["resources-task-0863329b4b49"])
        self.set_kudu_no_deployment()
        self.rest_client.post.return_value = MagicMock(status=202)

        def correct_settings(resource_group, function_app_name):
            return mock(properties={"WEBSITE_CONTENTSHARE": "resources-task-0863329b4b49123412341234"})

        self.web_client.web_apps.list_application_settings = AsyncMock(side_effect=correct_settings)
        self.web_client.web_apps.update_application_settings = AsyncMock()

        await self.run_deployer_task()

        self.web_client.web_apps.update_application_settings.assert_not_awaited()
        self.assertEqual(
            self.rest_client.post.mock_calls[0][1][0],
            "https://resources-task-0863329b4b49.scm.azurewebsites.net/api/zipdeploy?isAsync=true",
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

    # -------------------------------------------------------------------------
    # upload_function_app_data tests
    # -------------------------------------------------------------------------

    async def test_upload_function_app_data_succeeds_on_202(self):
        self.rest_client.post.return_value = MagicMock(status=202)
        task = DeployerTask()
        # Should not raise
        await task.upload_function_app_data("resources-task-0863329b4b49", b"data")
        self.rest_client.post.assert_awaited_once()

    async def test_upload_function_app_data_raises_on_non_202(self):
        self.rest_client.post.return_value = MagicMock(status=400, reason="Bad Request")
        self.rest_client.post.return_value.content.read = AsyncMock(return_value=b"error body")
        task = DeployerTask()
        with self.assertRaises(DeployError) as ctx:
            await task.upload_function_app_data("resources-task-0863329b4b49", b"data")
        self.assertIn("expected 202, got 400", str(ctx.exception))

    # -------------------------------------------------------------------------
    # check_deployment_status tests
    # -------------------------------------------------------------------------

    async def test_check_deployment_status_in_progress(self):
        task = DeployerTask()
        for in_progress_status in [0, 1, 2]:
            self.rest_client.get.return_value.json = AsyncMock(
                return_value={"complete": False, "status": in_progress_status}
            )
            is_complete, is_successful = await task.check_deployment_status(RESOURCES_STATUS_URL)
            self.assertFalse(is_complete)
            self.assertFalse(is_successful)

    async def test_check_deployment_status_success(self):
        self.rest_client.get.return_value.json = AsyncMock(return_value={"complete": True, "status": 4})
        task = DeployerTask()
        is_complete, is_successful = await task.check_deployment_status(RESOURCES_STATUS_URL)
        self.assertTrue(is_complete)
        self.assertTrue(is_successful)

    async def test_check_deployment_status_failed(self):
        self.rest_client.get.return_value.json = AsyncMock(return_value={"complete": True, "status": 3})
        task = DeployerTask()
        is_complete, is_successful = await task.check_deployment_status(RESOURCES_STATUS_URL)
        self.assertTrue(is_complete)
        self.assertFalse(is_successful)

    async def test_check_deployment_status_not_found(self):
        """404 → no deployment exists, treat as (complete=True, successful=False) to trigger a new deploy."""
        self.rest_client.get.return_value.ok = False
        self.rest_client.get.return_value.status = 404
        task = DeployerTask()
        is_complete, is_successful = await task.check_deployment_status(RESOURCES_STATUS_URL)
        self.assertTrue(is_complete)
        self.assertFalse(is_successful)

    async def test_check_deployment_status_http_error(self):
        self.rest_client.get.return_value.ok = False
        self.rest_client.get.return_value.status = 500
        self.rest_client.get.return_value.reason = "Internal Server Error"
        self.rest_client.get.return_value.content.read = AsyncMock(return_value=b"server error")
        task = DeployerTask()
        with self.assertRaises(DeployError):
            await task.check_deployment_status(RESOURCES_STATUS_URL)

    # -------------------------------------------------------------------------
    # deploy_component state machine tests
    # -------------------------------------------------------------------------

    async def test_deploy_component_kudu_in_progress_skips(self):
        """Deployment already in progress: no new deploy, manifest unchanged."""
        self.set_caches(
            public_cache={"resources": "2", "scaling": "1", "diagnostic_settings": "1"},
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_function_apps(ALL_FUNCTIONS)
        self.set_kudu_in_progress()

        task = await self.run_deployer_task()

        self.rest_client.post.assert_not_awaited()
        self.assertEqual(task.manifest_cache["resources"], "1")
        self.write_cache.assert_not_awaited()

    async def test_deploy_component_kudu_success_syncs_and_updates_manifest(self):
        """Kudu shows success: sync triggers, update manifest, no new zip deploy."""
        self.set_caches(
            public_cache={"resources": "2", "scaling": "1", "diagnostic_settings": "1"},
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_function_apps(ALL_FUNCTIONS)

        task = await self.run_deployer_task()

        zip_calls = [c for c in self.rest_client.post.call_args_list if "zipdeploy" in str(c)]
        self.assertFalse(zip_calls)
        self.assertEqual(task.manifest_cache["resources"], "2")
        self.assertEqual(self.cache["resources"], "2")

    async def test_deploy_component_kudu_failed_starts_new_deploy(self):
        """Kudu shows failure: start a new deployment."""
        self.set_caches(
            public_cache={"resources": "2", "scaling": "1", "diagnostic_settings": "1"},
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_function_apps(ALL_FUNCTIONS)
        self.rest_client.get.return_value.json = AsyncMock(return_value={"complete": True, "status": 3})
        self.rest_client.post.return_value = MagicMock(status=202)

        task = await self.run_deployer_task()

        zip_calls = [c for c in self.rest_client.post.call_args_list if "zipdeploy" in str(c)]
        self.assertTrue(zip_calls)
        # Manifest not updated (deployment just started)
        self.assertEqual(task.manifest_cache["resources"], "1")
        self.write_cache.assert_not_awaited()

    async def test_deploy_component_sync_triggers_fails_no_manifest_update(self):
        """Sync triggers fails after Kudu success: manifest NOT updated."""
        self.set_caches(
            public_cache={"resources": "2", "scaling": "1", "diagnostic_settings": "1"},
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_function_apps(ALL_FUNCTIONS)
        # Kudu success, but sync triggers POST fails (all 5 retries)
        self.rest_client.post.return_value = MagicMock(ok=False, status=500, reason="Error")
        self.rest_client.post.return_value.content.read = AsyncMock(return_value=b"")

        task = await self.run_deployer_task()

        self.assertEqual(task.manifest_cache["resources"], "1")
        self.write_cache.assert_not_awaited()

    async def test_deploy_component_status_check_error_attempts_deploy(self):
        """Kudu check raises DeployError (e.g. 500): fall through to attempting a new deployment."""
        self.set_caches(
            public_cache={"resources": "2", "scaling": "1", "diagnostic_settings": "1"},
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_function_apps(ALL_FUNCTIONS)
        self.rest_client.get.return_value.ok = False
        self.rest_client.get.return_value.status = 500
        self.rest_client.get.return_value.reason = "Error"
        self.rest_client.get.return_value.content.read = AsyncMock(return_value=b"")
        self.rest_client.post.return_value = MagicMock(status=202)

        await self.run_deployer_task()

        zip_calls = [c for c in self.rest_client.post.call_args_list if "zipdeploy" in str(c)]
        self.assertTrue(zip_calls)
