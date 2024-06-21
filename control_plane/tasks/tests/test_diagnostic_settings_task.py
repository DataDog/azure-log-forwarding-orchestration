from json import dumps, loads
from typing import cast
from unittest.mock import ANY, AsyncMock, Mock
from uuid import UUID

from tasks.diagnostic_settings_task import (
    DIAGNOSTIC_SETTING_PREFIX,
    DIAGNOSTIC_SETTINGS_TASK_NAME,
    DiagnosticSettingsTask,
)
from cache.diagnostic_settings_cache import (
    DIAGNOSTIC_SETTINGS_CACHE_BLOB,
    DiagnosticSettingsCache,
)
from cache.resources_cache import ResourceCache
from azure.mgmt.monitor.models import CategoryType

from tasks.tests.common import TaskTestCase, async_generator
from cache.tests import TEST_EVENT_HUB_NAME, TEST_EVENT_HUB_NAMESPACE


sub_id = "sub1"
resource_id = "/subscriptions/1/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1"


class TestAzureDiagnosticSettingsTask(TaskTestCase):
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

    async def run_diagnostic_settings_task(
        self, resource_cache: ResourceCache, diagnostic_settings_cache: DiagnosticSettingsCache
    ):
        async with DiagnosticSettingsTask(
            dumps(resource_cache, default=list), dumps(diagnostic_settings_cache)
        ) as task:
            await task.run()

    async def test_task_adds_missing_settings(self):
        self.list_diagnostic_settings.return_value = async_generator()
        self.list_diagnostic_settings_categories.return_value = async_generator(
            Mock(name="cool_logs", category_type=CategoryType.LOGS)
        )

        await self.run_diagnostic_settings_task(
            resource_cache={
                sub_id: {
                    resource_id,
                }
            },
            diagnostic_settings_cache={},
        )

        self.create_or_update_setting.assert_awaited()
        self.create_or_update_setting.assert_called_once_with(resource_id, ANY, ANY)
        setting = cast(DiagnosticSettingsCache, loads(self.cache_value(DIAGNOSTIC_SETTINGS_CACHE_BLOB)))[sub_id][
            resource_id
        ]
        self.assertEqual(str(UUID(setting["id"])), setting["id"])
        self.assertEqual(setting["event_hub_name"], TEST_EVENT_HUB_NAME)
        self.assertEqual(setting["event_hub_namespace"], TEST_EVENT_HUB_NAMESPACE)

    async def test_task_leaves_existing_settings_unchanged(self):
        setting_id = "12345"

        self.list_diagnostic_settings.return_value = async_generator(
            Mock(name=DIAGNOSTIC_SETTING_PREFIX + setting_id, event_hub_name=TEST_EVENT_HUB_NAME)
        )
        self.list_diagnostic_settings_categories.return_value = async_generator()

        await self.run_diagnostic_settings_task(
            resource_cache={sub_id: {resource_id}},
            diagnostic_settings_cache={
                sub_id: {
                    resource_id: {
                        "id": setting_id,
                        "event_hub_name": TEST_EVENT_HUB_NAME,
                        "event_hub_namespace": TEST_EVENT_HUB_NAMESPACE,
                    }
                }
            },
        )
        self.create_or_update_setting.assert_not_called()
        self.write_cache.assert_not_called()

    def test_malformed_resources_cache_errors_in_constructor(self):
        with self.assertRaises(ValueError) as e:
            DiagnosticSettingsTask("malformed", "{}")
        self.assertEqual(
            str(e.exception), "Resource Cache is in an invalid format, failing this task until it is valid"
        )
