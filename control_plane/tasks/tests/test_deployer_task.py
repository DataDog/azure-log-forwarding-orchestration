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
from cache.manifest_cache import (
    MANIFEST_CACHE_NAME,
    PENDING_DEPLOYMENTS_CACHE_NAME,
    ManifestCache,
    PendingDeployment,
    deserialize_manifest_cache,
    deserialize_pending_deployments,
)
from tasks.deployer_task import DEPLOYER_TASK_NAME, DeployError, DeployerTask
from tasks.tests.common import AsyncMockClient, TaskTestCase, async_generator, mock
from tasks.version import VERSION

ALL_FUNCTIONS = [
    "resources-task-0863329b4b49",
    "scaling-task-0863329b4b49",
    "diagnostic-settings-task-0863329b4b49",
]

RESOURCES_POLL_URL = "https://resources-task-0863329b4b49.scm.azurewebsites.net/api/deployments/latest"
SCALING_POLL_URL = "https://scaling-task-0863329b4b49.scm.azurewebsites.net/api/deployments/latest"
DS_POLL_URL = "https://diagnostic-settings-task-0863329b4b49.scm.azurewebsites.net/api/deployments/latest"


def make_pending_state(*pending: PendingDeployment) -> str:
    return dumps({p.component: {"component": p.component, "function_app": p.function_app,
                                "poll_url": p.poll_url, "target_manifest_hash": p.target_manifest_hash}
                  for p in pending})


class TestDeployerTask(TaskTestCase):
    TASK_NAME = DEPLOYER_TASK_NAME

    @property
    def cache(self) -> ManifestCache:
        return self.cache_value(MANIFEST_CACHE_NAME, deserialize_manifest_cache)

    @property
    def pending_cache(self):
        return self.cache_value(PENDING_DEPLOYMENTS_CACHE_NAME, deserialize_pending_deployments)

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
        self.rest_client.post.return_value = MagicMock(
            status=202,
            headers={"Location": RESOURCES_POLL_URL},
        )
        self.rest_client.get.return_value.ok = True
        self.rest_client.get.return_value.json = AsyncMock(return_value={"complete": True, "status": 4})
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

    async def run_deployer_task(self, pending_deployments_state: str = "") -> DeployerTask:
        async with DeployerTask(pending_deployments_state, is_initial_run=False) as task:
            await task.run()
        return task

    # -------------------------------------------------------------------------
    # Existing integration tests
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

    async def test_deploy_task_starts_async_deploy(self):
        """First run with a diff: starts async deploy, writes pending, does NOT update manifest."""
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

        # async URL used
        self.assertEqual(
            self.rest_client.post.mock_calls[0][1][0],
            "https://resources-task-0863329b4b49.scm.azurewebsites.net/api/zipdeploy?isAsync=true",
        )
        # pending written, manifest NOT written
        pending = self.pending_cache
        self.assertIn("resources", pending)
        self.assertEqual(pending["resources"].target_manifest_hash, "2")
        manifest_written_calls = [c for c in self.write_cache.call_args_list if c.args[0] == MANIFEST_CACHE_NAME]
        self.assertFalse(manifest_written_calls)

    async def test_deploy_task_diff_func_app(self):
        """Second run (pending exists): completes deploy, updates manifest."""
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
        pending_state = make_pending_state(
            PendingDeployment("resources", "resources-task-0863329b4b49", RESOURCES_POLL_URL, "2")
        )

        await self.run_deployer_task(pending_state)

        self.assertEqual(self.cache, public_cache)
        # sync_function_app_triggers uses POST; no zip deploy POST
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
        pending_state = make_pending_state(
            PendingDeployment("resources", "resources-task-0863329b4b49", RESOURCES_POLL_URL, "2")
        )

        await self.run_deployer_task(pending_state)

        self.assertEqual(self.cache, public_cache)

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

        # Pre-populate resources pending; diagnostic_settings will fail to start again this run
        pending_state = make_pending_state(
            PendingDeployment("resources", "resources-task-0863329b4b49", RESOURCES_POLL_URL, "2")
        )

        await self.run_deployer_task(pending_state)

        # resources completed, diagnostic_settings failed → only resources updated
        self.assertEqual(
            self.cache,
            {
                "resources": "2",
                "scaling": "1",
                "diagnostic_settings": "1",
            },
        )
        # diagnostic_settings download was retried 5 times
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
            "scaling": "2",
            "diagnostic_settings": "2",
        }
        self.public_client.download_blob.return_value.readall.return_value = dumps(public_cache).encode()
        self.read_private_cache.return_value = ""
        self.set_current_function_apps(ALL_FUNCTIONS)

        pending_state = make_pending_state(
            PendingDeployment("resources", "resources-task-0863329b4b49", RESOURCES_POLL_URL, "2"),
            PendingDeployment("scaling", "scaling-task-0863329b4b49", SCALING_POLL_URL, "2"),
            PendingDeployment("diagnostic_settings", "diagnostic-settings-task-0863329b4b49", DS_POLL_URL, "2"),
        )

        await self.run_deployer_task(pending_state)

        self.assertEqual(self.cache, public_cache)

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
            "scaling": "2",
            "diagnostic_settings": "2",
        }
        self.public_client.download_blob.return_value.readall.return_value = dumps(public_cache).encode()
        self.read_private_cache.side_effect = HttpResponseError
        self.set_current_function_apps(ALL_FUNCTIONS)

        pending_state = make_pending_state(
            PendingDeployment("resources", "resources-task-0863329b4b49", RESOURCES_POLL_URL, "2"),
            PendingDeployment("scaling", "scaling-task-0863329b4b49", SCALING_POLL_URL, "2"),
            PendingDeployment("diagnostic_settings", "diagnostic-settings-task-0863329b4b49", DS_POLL_URL, "2"),
        )

        await self.run_deployer_task(pending_state)

        self.assertEqual(self.read_private_cache.await_count, 5)
        self.assertEqual(self.cache, public_cache)

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
        self.rest_client.post.return_value = MagicMock(status=400)
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
        gov_poll_url = "https://resources-task-0863329b4b49.scm.azurewebsites.us/api/deployments/latest"
        pending_state = make_pending_state(
            PendingDeployment("resources", "resources-task-0863329b4b49", gov_poll_url, "2")
        )

        await self.run_deployer_task(pending_state)

        self.credential.get_token.assert_awaited_once_with("https://management.usgovcloudapi.net/.default")

        self.assertEqual(self.cache, public_cache)

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

    async def test_upload_function_app_data_returns_poll_url(self):
        poll_url = "https://resources-task-0863329b4b49.scm.azurewebsites.net/api/deployments/latest"
        self.rest_client.post.return_value = MagicMock(
            status=202,
            headers={"Location": poll_url},
        )
        task = DeployerTask()
        result = await task.upload_function_app_data("resources-task-0863329b4b49", b"data")
        self.assertEqual(result, poll_url)

    async def test_upload_function_app_data_raises_on_non_202(self):
        self.rest_client.post.return_value = MagicMock(
            status=400,
            reason="Bad Request",
        )
        self.rest_client.post.return_value.content.read = AsyncMock(return_value=b"error body")
        task = DeployerTask()
        with self.assertRaises(DeployError) as ctx:
            await task.upload_function_app_data("resources-task-0863329b4b49", b"data")
        self.assertIn("expected 202, got 400", str(ctx.exception))

    async def test_upload_function_app_data_raises_on_missing_location(self):
        self.rest_client.post.return_value = MagicMock(
            status=202,
            headers={},
        )
        task = DeployerTask()
        with self.assertRaises(DeployError) as ctx:
            await task.upload_function_app_data("resources-task-0863329b4b49", b"data")
        self.assertIn("no Location header", str(ctx.exception))

    # -------------------------------------------------------------------------
    # check_deployment_status tests
    # -------------------------------------------------------------------------

    async def test_check_deployment_status_in_progress(self):
        task = DeployerTask()
        for in_progress_status in [0, 1, 2]:
            self.rest_client.get.return_value.json = AsyncMock(
                return_value={"complete": False, "status": in_progress_status}
            )
            is_complete, is_successful = await task.check_deployment_status(RESOURCES_POLL_URL)
            self.assertFalse(is_complete)
            self.assertFalse(is_successful)

    async def test_check_deployment_status_success(self):
        self.rest_client.get.return_value.json = AsyncMock(return_value={"complete": True, "status": 4})
        task = DeployerTask()
        is_complete, is_successful = await task.check_deployment_status(RESOURCES_POLL_URL)
        self.assertTrue(is_complete)
        self.assertTrue(is_successful)

    async def test_check_deployment_status_failed(self):
        self.rest_client.get.return_value.json = AsyncMock(return_value={"complete": True, "status": 3})
        task = DeployerTask()
        is_complete, is_successful = await task.check_deployment_status(RESOURCES_POLL_URL)
        self.assertTrue(is_complete)
        self.assertFalse(is_successful)

    async def test_check_deployment_status_http_error(self):
        self.rest_client.get.return_value.ok = False
        self.rest_client.get.return_value.status = 500
        self.rest_client.get.return_value.reason = "Internal Server Error"
        self.rest_client.get.return_value.content.read = AsyncMock(return_value=b"server error")
        task = DeployerTask()
        with self.assertRaises(DeployError):
            await task.check_deployment_status(RESOURCES_POLL_URL)

    # -------------------------------------------------------------------------
    # deploy_component state machine tests
    # -------------------------------------------------------------------------

    async def test_deploy_component_no_pending_starts_deploy(self):
        """No pending deployment: starts async deploy, populates pending_deployments, does NOT sync or update manifest."""
        public_cache: ManifestCache = {"resources": "2", "scaling": "1", "diagnostic_settings": "1"}
        self.set_caches(
            public_cache=public_cache,
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_function_apps(ALL_FUNCTIONS)

        task = await self.run_deployer_task()

        self.rest_client.post.assert_awaited_once()
        self.rest_client.get.assert_not_awaited()
        # sync triggers was NOT called (only POST to zipdeploy)
        post_url = self.rest_client.post.call_args[0][0]
        self.assertIn("zipdeploy?isAsync=true", post_url)
        # pending_deployments written
        pending = self.pending_cache
        self.assertIn("resources", pending)
        # manifest NOT updated
        self.assertEqual(task.manifest_cache["resources"], "1")

    async def test_deploy_component_pending_in_progress(self):
        """Pending deployment still in progress: no new POST, preserves pending."""
        public_cache: ManifestCache = {"resources": "2", "scaling": "1", "diagnostic_settings": "1"}
        self.set_caches(
            public_cache=public_cache,
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_function_apps(ALL_FUNCTIONS)
        self.rest_client.get.return_value.json = AsyncMock(return_value={"complete": False, "status": 1})
        pending_state = make_pending_state(
            PendingDeployment("resources", "resources-task-0863329b4b49", RESOURCES_POLL_URL, "2")
        )

        task = await self.run_deployer_task(pending_state)

        self.rest_client.post.assert_not_awaited()
        self.rest_client.get.assert_awaited_once()
        # pending preserved (still in pending_deployments)
        self.assertIn("resources", task.pending_deployments)
        # manifest NOT updated
        self.assertEqual(task.manifest_cache["resources"], "1")
        # Nothing written (pending unchanged from initial state)
        self.write_cache.assert_not_awaited()

    async def test_deploy_component_pending_succeeds(self):
        """Pending deployment succeeds: syncs triggers, updates manifest, clears pending."""
        public_cache: ManifestCache = {"resources": "2", "scaling": "1", "diagnostic_settings": "1"}
        self.set_caches(
            public_cache=public_cache,
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_function_apps(ALL_FUNCTIONS)
        self.rest_client.get.return_value.json = AsyncMock(return_value={"complete": True, "status": 4})
        pending_state = make_pending_state(
            PendingDeployment("resources", "resources-task-0863329b4b49", RESOURCES_POLL_URL, "2")
        )

        task = await self.run_deployer_task(pending_state)

        self.rest_client.post.assert_awaited()  # sync triggers calls POST
        self.rest_client.get.assert_awaited_once()
        self.assertNotIn("resources", task.pending_deployments)
        self.assertEqual(task.manifest_cache["resources"], "2")
        self.assertEqual(self.cache, public_cache)

    async def test_deploy_component_pending_fails(self):
        """Pending deployment fails: clears pending, does NOT update manifest."""
        public_cache: ManifestCache = {"resources": "2", "scaling": "1", "diagnostic_settings": "1"}
        self.set_caches(
            public_cache=public_cache,
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_function_apps(ALL_FUNCTIONS)
        self.rest_client.get.return_value.json = AsyncMock(return_value={"complete": True, "status": 3})
        pending_state = make_pending_state(
            PendingDeployment("resources", "resources-task-0863329b4b49", RESOURCES_POLL_URL, "2")
        )

        task = await self.run_deployer_task(pending_state)

        self.assertNotIn("resources", task.pending_deployments)
        self.assertEqual(task.manifest_cache["resources"], "1")
        # manifest NOT written (unchanged), pending IS written (cleared)
        pending_written_calls = [c for c in self.write_cache.call_args_list
                                 if c.args[0] == PENDING_DEPLOYMENTS_CACHE_NAME]
        self.assertTrue(pending_written_calls)
        manifest_written_calls = [c for c in self.write_cache.call_args_list
                                   if c.args[0] == MANIFEST_CACHE_NAME]
        self.assertFalse(manifest_written_calls)

    async def test_deploy_component_status_check_error_preserves_pending(self):
        """Status check HTTP error: preserves pending, does not crash."""
        public_cache: ManifestCache = {"resources": "2", "scaling": "1", "diagnostic_settings": "1"}
        self.set_caches(
            public_cache=public_cache,
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_function_apps(ALL_FUNCTIONS)
        self.rest_client.get.return_value.ok = False
        self.rest_client.get.return_value.status = 500
        self.rest_client.get.return_value.reason = "Server Error"
        self.rest_client.get.return_value.content.read = AsyncMock(return_value=b"")
        pending_state = make_pending_state(
            PendingDeployment("resources", "resources-task-0863329b4b49", RESOURCES_POLL_URL, "2")
        )

        task = await self.run_deployer_task(pending_state)

        # Pending preserved
        self.assertIn("resources", task.pending_deployments)
        # Nothing written
        self.write_cache.assert_not_awaited()

    async def test_deploy_component_sync_triggers_fails_no_manifest_update(self):
        """Sync triggers fails after successful deploy: manifest NOT updated, pending cleared."""
        public_cache: ManifestCache = {"resources": "2", "scaling": "1", "diagnostic_settings": "1"}
        self.set_caches(
            public_cache=public_cache,
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_function_apps(ALL_FUNCTIONS)
        self.rest_client.get.return_value.json = AsyncMock(return_value={"complete": True, "status": 4})
        # sync triggers POST fails (all 5 retries)
        self.rest_client.post.return_value = MagicMock(ok=False, status=500, reason="Error")
        self.rest_client.post.return_value.content.read = AsyncMock(return_value=b"")
        pending_state = make_pending_state(
            PendingDeployment("resources", "resources-task-0863329b4b49", RESOURCES_POLL_URL, "2")
        )

        task = await self.run_deployer_task(pending_state)

        # Pending cleared (deployment completed, even though triggers failed)
        self.assertNotIn("resources", task.pending_deployments)
        # Manifest NOT updated
        self.assertEqual(task.manifest_cache["resources"], "1")
