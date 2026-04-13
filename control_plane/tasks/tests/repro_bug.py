# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

from json import dumps
from unittest.mock import Mock, patch
from azure.mgmt.monitor.models import CategoryType
from tasks.diagnostic_settings_task import DIAGNOSTIC_SETTING_PREFIX, DiagnosticSettingsTask
from tasks.tests.common import TaskTestCase, async_generator, mock, AzureModelMatcher
from cache.common import STORAGE_ACCOUNT_TYPE

sub_id1 = "sub1"
region1 = "region1"
config_id1 = "bc666ef914ec"
control_plane_id = "e90ecb54476d"
DIAGNOSTIC_SETTING_NAME = DIAGNOSTIC_SETTING_PREFIX + control_plane_id
resource_id1 = "/subscriptions/1/resourcegroups/rg1/providers/microsoft.compute/virtualmachines/vm1"

class TestDiagnosticSettingsBug(TaskTestCase):
    TASK_NAME = "diagnostic_settings_task"
    def setUp(self) -> None:
        super().setUp()
        self.patch_path("tasks.task.DatadogClient")
        self.client = self.patch("MonitorManagementClient").return_value.__aenter__.return_value
        self.client.diagnostic_settings.list = Mock()
        self.list_settings = self.client.diagnostic_settings.list
        self.client.diagnostic_settings_category.list = Mock()
        self.list_categories = self.client.diagnostic_settings_category.list
        self.create_or_update = self.client.diagnostic_settings.create_or_update
        
        with patch.dict("os.environ", {"RESOURCE_GROUP": "lfo", "CONTROL_PLANE_ID": control_plane_id}):
            pass

    async def test_bug_reenables_all_categories_when_empty(self):
        # GIVEN: An existing diagnostic setting with NO enabled logs
        self.list_settings.return_value = async_generator(
            mock(
                name=DIAGNOSTIC_SETTING_NAME,
                storage_account_id="wrong_id",
                logs=[] # No logs enabled
            )
        )
        # AND: The resource has some categories available
        self.list_categories.return_value = async_generator(
            mock(name="category1", category_type=CategoryType.LOGS),
            mock(name="category2", category_type=CategoryType.LOGS)
        )

        # WHEN: Running the task
        async with DiagnosticSettingsTask(
            dumps({}),
            dumps({sub_id1: {region1: {"configurations": {config_id1: STORAGE_ACCOUNT_TYPE}, "resources": {resource_id1: config_id1}}}}),
            dumps({})
        ) as task:
            await task.run()

        # THEN: It should NOT have re-enabled all categories!
        # It should have returned False and not called create_or_update because categories is []
        self.create_or_update.assert_not_awaited()

    async def test_bug_reenables_disabled_categories(self):
        # GIVEN: An existing diagnostic setting with one enabled and one disabled category
        self.list_settings.return_value = async_generator(
            mock(
                name=DIAGNOSTIC_SETTING_NAME,
                storage_account_id="wrong_id",
                logs=[
                    mock(category="enabled_cat", enabled=True),
                    mock(category="disabled_cat", enabled=False)
                ]
            )
        )

        # WHEN: Running the task
        async with DiagnosticSettingsTask(
            dumps({}),
            dumps({sub_id1: {region1: {"configurations": {config_id1: STORAGE_ACCOUNT_TYPE}, "resources": {resource_id1: config_id1}}}}),
            dumps({})
        ) as task:
            await task.run()

        # THEN: It should ONLY have kept the enabled one
        self.create_or_update.assert_awaited_once()
        args = self.create_or_update.call_args[0]
        sent_settings = args[2]
        
        sent_categories = [l.category for l in sent_settings.logs]
        self.assertIn("enabled_cat", sent_categories)
        self.assertNotIn("disabled_cat", sent_categories, "Should NOT have re-enabled disabled category")
