from json import dumps, loads
from typing import AsyncIterable, TypeVar, cast
from unittest.mock import ANY, AsyncMock, Mock, patch
from diagnostic_settings_task.function_app import (
    DIAGNOSTIC_SETTING_PREFIX,
    EVENT_HUB_NAME_SETTING,
    EVENT_HUB_NAMESPACE_SETTING,
    DiagnosticSettingsTask,
    environ,
)
from cache.diagnostic_settings_cache import (
    UUID_REGEX,
    DiagnosticSettingsCache,
)
from cache.resources_cache import ResourceCache
from unittest import IsolatedAsyncioTestCase
from azure.mgmt.monitor.models import CategoryType


T = TypeVar("T")


async def agen(*items: T) -> AsyncIterable[T]:
    for x in items:
        yield x


TEST_EVENT_HUB_NAME = "test_event_hub"
TEST_EVENT_HUB_NAMESPACE = "test_event_hub_namespace"


sub_id = "sub1"
resource_id = "/subscriptions/1/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1"


class TestAzureDiagnosticSettingsCrawler(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        m = patch("diagnostic_settings_task.function_app.MonitorManagementClient")
        self.addCleanup(m.stop)
        client: AsyncMock = m.start().return_value.__aenter__.return_value
        client.diagnostic_settings.list = Mock()
        self.list_diagnostic_settings: Mock = client.diagnostic_settings.list
        client.diagnostic_settings_category.list = Mock()
        self.list_diagnostic_settings_categories: Mock = client.diagnostic_settings_category.list
        self.create_or_update_setting: AsyncMock = client.diagnostic_settings.create_or_update
        client.subscription_diagnostic_settings.list = Mock(return_value=agen())  # nothing to test here yet
        self.credential = AsyncMock()
        self.out_mock = Mock()

    @property
    def out_value(self):
        return self.out_mock.set.call_args[0][0]

    def run_diagnostic_settings_task(
        self, resource_cache: ResourceCache, diagnostic_settings_cache: DiagnosticSettingsCache
    ):
        return DiagnosticSettingsTask(
            self.credential, dumps(resource_cache, default=list), dumps(diagnostic_settings_cache), self.out_mock
        ).run()

    @patch.dict(
        environ, {EVENT_HUB_NAME_SETTING: TEST_EVENT_HUB_NAME, EVENT_HUB_NAMESPACE_SETTING: TEST_EVENT_HUB_NAMESPACE}
    )
    async def test_task_adds_missing_settings(self):
        self.list_diagnostic_settings.return_value = agen()
        self.list_diagnostic_settings_categories.return_value = agen(
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
        self.out_mock.set.assert_called_once()
        setting = cast(DiagnosticSettingsCache, loads(self.out_value))[sub_id][resource_id]
        self.assertRegex(setting["id"], UUID_REGEX)
        self.assertEqual(setting["event_hub_name"], TEST_EVENT_HUB_NAME)
        self.assertEqual(setting["event_hub_namespace"], TEST_EVENT_HUB_NAMESPACE)

    @patch.dict(
        environ, {EVENT_HUB_NAME_SETTING: TEST_EVENT_HUB_NAME, EVENT_HUB_NAMESPACE_SETTING: TEST_EVENT_HUB_NAMESPACE}
    )
    async def test_task_leaves_existing_settings_unchanged(self):
        setting_id = "12345"

        self.list_diagnostic_settings.return_value = agen(
            Mock(name=DIAGNOSTIC_SETTING_PREFIX + setting_id, event_hub_name=TEST_EVENT_HUB_NAME)
        )
        self.list_diagnostic_settings_categories.return_value = agen()

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
        self.create_or_update_setting.assert_not_awaited()
        self.out_mock.set.assert_not_called()

    def test_malformed_resources_cache_errors_in_constructor(self):
        with self.assertRaises(ValueError) as e:
            DiagnosticSettingsTask(self.credential, "malformed", "{}", self.out_mock)
        self.assertEqual(
            str(e.exception), "Resource Cache is in an invalid format, failing this task until it is valid"
        )
