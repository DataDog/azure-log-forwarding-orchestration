# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from os import environ

# project
from scripts.initial_run import main
from tasks.tests.common import AsyncMockClient, AsyncTestCase


def get_config_option(setting: str) -> str:
    return setting


class TestMain(AsyncTestCase):
    def setUp(self) -> None:
        super().setUp()
        environ["RESOURCE_GROUP"] = "test_resource_group"
        self.is_initial_deploy = self.patch_path("scripts.initial_run.is_initial_deploy")
        self.create_credential = self.patch_path("scripts.initial_run.create_credential")
        self.container_apps_client = AsyncMockClient()
        self.patch_path("scripts.initial_run.ContainerAppsAPIClient", return_value=self.container_apps_client)
        self.get_config_option = self.patch_path("scripts.initial_run.get_config_option")
        self.get_config_option.side_effect = get_config_option

        self.datadog_client = AsyncMockClient()
        self.patch_path("scripts.initial_run.DatadogClient", return_value=self.datadog_client)

        self.resources_task = AsyncMockClient()
        self.patch_path("scripts.initial_run.ResourcesTask", return_value=self.resources_task)

        self.scaling_task = AsyncMockClient()
        self.patch_path("scripts.initial_run.ScalingTask", return_value=self.scaling_task)

        self.diagnostic_settings_task = AsyncMockClient()
        self.patch_path("scripts.initial_run.DiagnosticSettingsTask", return_value=self.diagnostic_settings_task)

    async def test_main_runs_all_tasks_on_initial_run(self):
        # GIVEN
        self.is_initial_deploy.return_value = True
        self.scaling_task.assignment_cache = {}

        # WHEN
        await main()

        # THEN
        self.container_apps_client.jobs.begin_start.assert_awaited_once()
        self.assertTrue(self.get_config_option.called)
        self.assertTrue(self.create_credential.called)

        self.resources_task.run.assert_awaited_once()
        self.scaling_task.run.assert_awaited()
        self.diagnostic_settings_task.run.assert_awaited_once()

        self.datadog_client.submit_status_update.assert_awaited()

    async def test_main_runs_deployer_on_follow_up_runs(self):
        # GIVEN
        self.is_initial_deploy.return_value = False

        # WHEN
        await main()

        # THEN
        self.container_apps_client.jobs.begin_start.assert_awaited()

    async def test_main_uses_gov_cloud_endpoint_for_gov_regions(self):
        # GIVEN
        self.is_initial_deploy.return_value = False
        environ["CONTROL_PLANE_REGION"] = "usgovvirginia"

        # Mock the get_config_option to return actual values for region
        def get_config_option_with_region(setting: str) -> str:
            if setting == "CONTROL_PLANE_REGION":
                return "usgovvirginia"
            return setting

        self.get_config_option.side_effect = get_config_option_with_region
        container_apps_client_mock = self.patch_path("scripts.initial_run.ContainerAppsAPIClient")
        container_apps_client_mock.return_value = self.container_apps_client

        # WHEN
        await main()

        # THEN
        # Verify ContainerAppsAPIClient was called with gov cloud base_url and credential_scopes
        container_apps_client_mock.assert_called_once()
        call_kwargs = container_apps_client_mock.call_args[1]
        self.assertEqual(call_kwargs["base_url"], "https://management.usgovcloudapi.net")
        self.assertEqual(call_kwargs["credential_scopes"], ["https://management.usgovcloudapi.net/.default"])
        self.container_apps_client.jobs.begin_start.assert_awaited()

    async def test_main_uses_public_cloud_endpoint_for_public_regions(self):
        # GIVEN
        self.is_initial_deploy.return_value = False
        environ["CONTROL_PLANE_REGION"] = "eastus"

        # Mock the get_config_option to return actual values for region
        def get_config_option_with_region(setting: str) -> str:
            if setting == "CONTROL_PLANE_REGION":
                return "eastus"
            return setting

        self.get_config_option.side_effect = get_config_option_with_region
        container_apps_client_mock = self.patch_path("scripts.initial_run.ContainerAppsAPIClient")
        container_apps_client_mock.return_value = self.container_apps_client

        # WHEN
        await main()

        # THEN
        # Verify ContainerAppsAPIClient was called with public cloud base_url and credential_scopes
        container_apps_client_mock.assert_called_once()
        call_kwargs = container_apps_client_mock.call_args[1]
        self.assertEqual(call_kwargs["base_url"], "https://management.azure.com")
        self.assertEqual(call_kwargs["credential_scopes"], ["https://management.azure.com/.default"])
        self.container_apps_client.jobs.begin_start.assert_awaited()

    async def test_main_uses_gov_cloud_endpoint_for_usgovarizona(self):
        # GIVEN
        self.is_initial_deploy.return_value = False
        environ["CONTROL_PLANE_REGION"] = "usgovarizona"

        # Mock the get_config_option to return actual values for region
        def get_config_option_with_region(setting: str) -> str:
            if setting == "CONTROL_PLANE_REGION":
                return "usgovarizona"
            return setting

        self.get_config_option.side_effect = get_config_option_with_region
        container_apps_client_mock = self.patch_path("scripts.initial_run.ContainerAppsAPIClient")
        container_apps_client_mock.return_value = self.container_apps_client

        # WHEN
        await main()

        # THEN
        # Verify ContainerAppsAPIClient was called with gov cloud base_url
        container_apps_client_mock.assert_called_once()
        call_kwargs = container_apps_client_mock.call_args[1]
        self.assertEqual(call_kwargs["base_url"], "https://management.usgovcloudapi.net")
        self.assertEqual(call_kwargs["credential_scopes"], ["https://management.usgovcloudapi.net/.default"])
        self.container_apps_client.jobs.begin_start.assert_awaited()
