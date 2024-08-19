# stdlib
from json import dumps
from typing import Final
from unittest.mock import ANY, AsyncMock, Mock

# 3p
from azure.mgmt.monitor.models import CategoryType
from azure.mgmt.monitor.v2021_05_01_preview.models import DiagnosticSettingsResource

# project
from cache.assignment_cache import AssignmentCache
from cache.common import STORAGE_ACCOUNT_TYPE, InvalidCacheError
from cache.diagnostic_settings_cache import (
    DIAGNOSTIC_SETTINGS_CACHE_BLOB,
    DiagnosticSettingsCache,
    deserialize_diagnostic_settings_cache,
)
from cache.tests import TEST_EVENT_HUB_NAME
from tasks.diagnostic_settings_task import (
    DIAGNOSTIC_SETTING_PREFIX,
    DIAGNOSTIC_SETTINGS_TASK_NAME,
    DiagnosticSettingsTask,
)
from tasks.tests.common import TaskTestCase, UnexpectedException, async_generator, mock

sub_id1: Final = "sub1"
region1: Final = "region1"
config_id1: Final = "bc666ef914ec"
resource_id1: Final = "/subscriptions/1/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1"
storage_account1: Final = "/subscriptions/1/resourceGroups/lfo/providers/Microsoft.Storage/storageAccounts/storageacc1"


class TestDiagnosticSettingsTask(TaskTestCase):
    TASK_NAME = DIAGNOSTIC_SETTINGS_TASK_NAME

    def setUp(self) -> None:
        super().setUp()
        client: AsyncMock = self.patch("MonitorManagementClient").return_value.__aenter__.return_value
        client.diagnostic_settings.list = Mock()
        self.list_diagnostic_settings: Mock = client.diagnostic_settings.list
        client.diagnostic_settings_category.list = Mock()
        self.list_diagnostic_settings_categories: Mock = client.diagnostic_settings_category.list
        self.create_or_update_setting: AsyncMock = client.diagnostic_settings.create_or_update
        client.subscription_diagnostic_settings.list = Mock(return_value=async_generator())  # nothing to test here yet
        self.resource_group = "lfo"

    async def run_diagnostic_settings_task(
        self, assignment_cache: AssignmentCache, diagnostic_settings_cache: DiagnosticSettingsCache
    ):
        async with DiagnosticSettingsTask(
            dumps(assignment_cache), dumps(diagnostic_settings_cache), self.resource_group
        ) as task:
            await task.run()

    @property
    def cache(self) -> DiagnosticSettingsCache:
        return self.cache_value(DIAGNOSTIC_SETTINGS_CACHE_BLOB, deserialize_diagnostic_settings_cache)

    async def test_task_adds_missing_settings(self):
        self.list_diagnostic_settings.return_value = async_generator()
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS)
        )

        await self.run_diagnostic_settings_task(
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            diagnostic_settings_cache={},
        )

        # check the diagnostic setting was created
        self.create_or_update_setting.assert_awaited_once_with(
            resource_id1,
            "datadog_log_forwarding_bc666ef914ec",
            ANY,  # the azure sdk doesnt implement __eq__ so we have to check it separarely after
        )
        diagnostic_setting: DiagnosticSettingsResource = self.create_or_update_setting.call_args[0][2]
        self.assertEqual(
            diagnostic_setting.as_dict(),
            {
                "logs": [{"category": "cool_logs", "enabled": True}],
                "storage_account_id": "/subscriptions/sub1/resourceGroups/lfo/providers/Microsoft.Storage/storageAccounts/ddlogstoragebc666ef914ec",
            },
        )

        # check the cache was updated
        expected_cache: DiagnosticSettingsCache = {sub_id1: {resource_id1: config_id1}}
        self.assertEqual(self.cache, expected_cache)

    async def test_task_leaves_existing_settings_unchanged(self):
        config_id = "3168b2251a45"

        self.list_diagnostic_settings.return_value = async_generator(
            Mock(name=DIAGNOSTIC_SETTING_PREFIX + config_id, event_hub_name=TEST_EVENT_HUB_NAME)
        )
        self.list_diagnostic_settings_categories.return_value = async_generator()

        await self.run_diagnostic_settings_task(
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            diagnostic_settings_cache={sub_id1: {resource_id1: config_id1}},
        )
        self.create_or_update_setting.assert_not_awaited()
        self.write_cache.assert_not_awaited()

    def test_malformed_resources_cache_errors_in_constructor(self):
        with self.assertRaises(InvalidCacheError) as e:
            DiagnosticSettingsTask("malformed", "{}", "rg")
        self.assertEqual(
            str(e.exception), "Assignment Cache is in an invalid format, failing this task until it is valid"
        )

    async def test_unexpected_failure_skips_cache_write(self):
        self.patch("MonitorManagementClient").side_effect = UnexpectedException("unexpected")
        write_caches = self.patch("DiagnosticSettingsTask.write_caches")
        with self.assertRaises(UnexpectedException):
            async with DiagnosticSettingsTask(
                dumps(
                    {
                        sub_id1: {
                            region1: {
                                "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                                "resources": {resource_id1: config_id1},
                            }
                        }
                    }
                ),
                dumps({sub_id1: {resource_id1: config_id1}}),
                self.resource_group,
            ) as task:
                # change the cache so there is something to write
                task.diagnostic_settings_cache = {}
                await task.run()

        write_caches.assert_not_awaited()
