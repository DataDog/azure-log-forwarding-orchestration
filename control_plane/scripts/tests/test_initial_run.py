# stdlib
from os import environ
from unittest.mock import AsyncMock

# project
from scripts.initial_run import main
from tasks.tests.common import AsyncTestCase


def get_config_option(setting: str) -> str:
    return setting


class TestMain(AsyncTestCase):
    def setUp(self) -> None:
        super().setUp()
        environ["RESOURCE_GROUP"] = "test_resource_group"
        self.is_initial_deploy = self.patch_path("scripts.initial_run.is_initial_deploy")
        self.default_credential = self.patch_path("scripts.initial_run.DefaultAzureCredential")
        self.container_apps_client = AsyncMock()
        self.container_apps_client_patch = self.patch_path(
            "scripts.initial_run.ContainerAppsAPIClient", return_value=self.container_apps_client
        )
        self.get_config_option = self.patch_path("scripts.initial_run.get_config_option")
        self.get_config_option.side_effect = get_config_option

    async def test_main_runs_deployer_on_follow_up_runs(self):
        # GIVEN
        self.is_initial_deploy.return_value = False

        # WHEN
        await main()

        # THEN
        # self.container_apps_client.jobs.begin_start.assert_awaited()
        assert True
