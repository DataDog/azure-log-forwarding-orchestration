# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from json import dumps
from unittest.mock import AsyncMock, MagicMock

# project
from cache.common import InvalidCacheError
from cache.env import (
    CONTROL_PLANE_ID_SETTING,
    CONTROL_PLANE_REGION_SETTING,
    RESOURCE_GROUP_SETTING,
    SUBSCRIPTION_ID_SETTING,
)
from cache.manifest_cache import TASK_IMAGES_MANIFEST_FILE_NAME, ManifestCache, deserialize_manifest_cache
from tasks.container_app_jobs_deployer_task import CAJ_DEPLOYER_TASK_NAME, ContainerAppJobsDeployerTask
from tasks.tests.common import AsyncMockClient, AzureModelMatcher, TaskTestCase, async_generator, mock
from tasks.version import VERSION

ALL_JOBS = [
    "resources-task-0863329b4b49",
    "scaling-task-0863329b4b49",
    "diagnostic-settings-task-0863329b4b49",
]


class TestContainerAppJobsDeployerTask(TaskTestCase):
    TASK_NAME = CAJ_DEPLOYER_TASK_NAME

    @property
    def cache(self) -> ManifestCache:
        return self.cache_value(TASK_IMAGES_MANIFEST_FILE_NAME, deserialize_manifest_cache)

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
        self.patch("ClientSession").return_value = self.rest_client
        self.container_apps_client = AsyncMockClient()
        self.container_apps_client.jobs.list_by_resource_group = MagicMock(return_value=async_generator())
        self.container_apps_client.jobs.begin_update = AsyncMock(return_value=AsyncMock())
        self.patch("ContainerAppsAPIClient").return_value = self.container_apps_client

    def set_caches(self, public_cache: ManifestCache, private_cache: ManifestCache) -> None:
        self.public_client.download_blob.return_value.readall.return_value = dumps(public_cache).encode()
        self.read_private_cache.return_value = dumps(private_cache)

    def set_current_jobs(self, jobs: list[str]) -> None:
        self.container_apps_client.jobs.list_by_resource_group.return_value = async_generator(
            *(mock(name=job) for job in jobs)
        )

    async def run_task(self) -> ContainerAppJobsDeployerTask:
        async with ContainerAppJobsDeployerTask(is_initial_run=False) as task:
            await task.run()
        return task

    async def test_no_diff(self):
        self.set_caches(
            public_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_jobs(ALL_JOBS)

        await self.run_task()

        self.container_apps_client.jobs.begin_update.assert_not_awaited()
        self.write_cache.assert_not_awaited()

    async def test_updates_changed_component(self):
        self.set_caches(
            public_cache={"resources": "2", "scaling": "1", "diagnostic_settings": "1"},
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_jobs(ALL_JOBS)

        await self.run_task()

        self.container_apps_client.jobs.begin_update.assert_awaited_once_with(
            "test_rg",
            "resources-task-0863329b4b49",
            AzureModelMatcher(
                {"properties": {"template": {"containers": [{"name": "resources-task", "image": "2"}]}}}
            ),
        )
        self.assertEqual(self.cache, {"resources": "2", "scaling": "1", "diagnostic_settings": "1"})

    async def test_updates_multiple_components(self):
        self.set_caches(
            public_cache={"resources": "2", "scaling": "2", "diagnostic_settings": "1"},
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_jobs(ALL_JOBS)

        await self.run_task()

        self.assertEqual(self.container_apps_client.jobs.begin_update.await_count, 2)
        self.assertEqual(self.cache, {"resources": "2", "scaling": "2", "diagnostic_settings": "1"})

    async def test_no_private_manifest(self):
        public_cache: ManifestCache = {"resources": "2", "scaling": "1", "diagnostic_settings": "1"}
        self.public_client.download_blob.return_value.readall.return_value = dumps(public_cache).encode()
        self.read_private_cache.return_value = ""
        self.set_current_jobs(ALL_JOBS)

        await self.run_task()

        self.assertEqual(self.container_apps_client.jobs.begin_update.await_count, 3)
        self.assertEqual(self.cache, public_cache)

    async def test_invalid_public_manifest(self):
        self.public_client.download_blob.return_value.readall.return_value = b"invalid"
        self.read_private_cache.return_value = dumps({"resources": "1", "scaling": "1", "diagnostic_settings": "1"})
        self.set_current_jobs(ALL_JOBS)

        with self.assertRaises(InvalidCacheError) as ctx:
            await self.run_task()

        self.write_cache.assert_not_awaited()
        self.public_client.download_blob.assert_awaited_once_with(TASK_IMAGES_MANIFEST_FILE_NAME)
        self.assertEqual("Invalid Public Manifest: invalid", str(ctx.exception))

    async def test_private_manifest_error(self):
        public_cache: ManifestCache = {"resources": "2", "scaling": "1", "diagnostic_settings": "1"}
        self.public_client.download_blob.return_value.readall.return_value = dumps(public_cache).encode()
        self.read_private_cache.side_effect = Exception("storage error")
        self.set_current_jobs(ALL_JOBS)

        await self.run_task()

        self.assertEqual(self.container_apps_client.jobs.begin_update.await_count, 3)
        self.assertEqual(self.cache, public_cache)

    async def test_job_not_found_skips_component(self):
        self.set_caches(
            public_cache={"resources": "2", "scaling": "1", "diagnostic_settings": "1"},
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_jobs(["scaling-task-0863329b4b49", "diagnostic-settings-task-0863329b4b49"])

        await self.run_task()

        self.container_apps_client.jobs.begin_update.assert_not_awaited()
        self.write_cache.assert_not_awaited()

    async def test_update_failure_skips_cache_write_for_component(self):
        self.set_caches(
            public_cache={"resources": "2", "scaling": "2", "diagnostic_settings": "1"},
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_jobs(ALL_JOBS)
        self.container_apps_client.jobs.begin_update.side_effect = [Exception("update failed"), AsyncMock()]

        await self.run_task()

        self.assertEqual(self.container_apps_client.jobs.begin_update.await_count, 2)

    async def test_govcloud(self):
        self.env[CONTROL_PLANE_REGION_SETTING] = "usgovarizona"
        self.set_caches(
            public_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_jobs(ALL_JOBS)

        await self.run_task()

        self.credential.get_token.assert_awaited_once_with("https://management.usgovcloudapi.net/.default")
        self.container_apps_client.jobs.begin_update.assert_not_awaited()

    async def test_tags(self):
        self.env[CONTROL_PLANE_ID_SETTING] = "a2b4c5d6"
        self.set_caches(
            public_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
            private_cache={"resources": "1", "scaling": "1", "diagnostic_settings": "1"},
        )
        self.set_current_jobs(ALL_JOBS)

        task = await self.run_task()

        self.assertCountEqual(
            task.tags,
            [
                "forwarder:lfocontrolplane",
                "task:container_app_jobs_deployer_task",
                "control_plane_id:a2b4c5d6",
                f"version:{VERSION}",
            ],
        )
