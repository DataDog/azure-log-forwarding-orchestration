# stdlib
from collections.abc import AsyncIterable
from json import dumps
from os import environ
from typing import Final, cast
from unittest.mock import AsyncMock, Mock, patch

# 3p
from azure.core.exceptions import HttpResponseError
from azure.mgmt.monitor.models import CategoryType

# project
from cache.assignment_cache import AssignmentCache
from cache.common import STORAGE_ACCOUNT_TYPE, InvalidCacheError
from cache.diagnostic_settings_cache import DIAGNOSTIC_SETTINGS_COUNT, SENT_EVENT, DiagnosticSettingsCache, EventDict
from cache.resources_cache import ResourceCache
from cache.tests import TEST_EVENT_HUB_NAME
from tasks.diagnostic_settings_task import (
    DIAGNOSTIC_SETTING_PREFIX,
    DIAGNOSTIC_SETTINGS_TASK_NAME,
    MAX_DIAGNOSTIC_SETTINGS,
    DiagnosticSettingsTask,
)
from tasks.tests.common import AzureModelMatcher, TaskTestCase, async_generator, mock

sub_id1: Final = "sub1"
region1: Final = "region1"
config_id1: Final = "bc666ef914ec"
control_plane_id: Final = "e90ecb54476d"
DIAGNOSTIC_SETTING_NAME: Final = DIAGNOSTIC_SETTING_PREFIX + control_plane_id
resource_id1: Final = "/subscriptions/1/resourcegroups/rg1/providers/microsoft.compute/virtualmachines/vm1"
resource_id2: Final = "/subscriptions/1/resourcegroups/rg1/providers/microsoft.compute/virtualmachines/vm2"
storage_account: Final = (
    f"/subscriptions/{sub_id1}/resourcegroups/lfo/providers/microsoft.storage/storageaccounts/ddlogstorage{config_id1}"
)


def mock_diagnostic_settings(count: int) -> AsyncIterable[Mock]:
    mocks = [mock(name=f"{i}-{DIAGNOSTIC_SETTING_NAME}", category_type=CategoryType.LOGS) for i in range(count)]
    return async_generator(*mocks)


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
        self.delete_diagnostic_setting: AsyncMock = client.diagnostic_settings.delete
        client.subscription_diagnostic_settings.list = Mock(return_value=async_generator())  # nothing to test here yet

        self.send_max_settings_reached_event = self.patch("DiagnosticSettingsTask.send_max_settings_reached_event")
        self.send_max_settings_reached_event.return_value = True
        self.log = self.patch_path("tasks.task.log").getChild.return_value
        env = patch.dict(environ, {"RESOURCE_GROUP": "lfo", "CONTROL_PLANE_ID": control_plane_id})
        env.start()
        self.addCleanup(env.stop)

    async def run_diagnostic_settings_task(
        self,
        resource_cache: ResourceCache | None,
        assignment_cache: AssignmentCache,
        event_cache: DiagnosticSettingsCache | None,
    ) -> DiagnosticSettingsTask:
        async with DiagnosticSettingsTask(
            dumps(resource_cache, default=list), dumps(assignment_cache), dumps(event_cache)
        ) as task:
            await task.run()
        return task

    def test_malformed_assignment_cache_errors_in_constructor(self):
        with self.assertRaises(InvalidCacheError) as e:
            DiagnosticSettingsTask("", "malformed", "")
        self.assertEqual(
            str(e.exception), "Assignment Cache is in an invalid format, failing this task until it is valid"
        )

    async def test_task_adds_missing_settings(self):
        self.list_diagnostic_settings.return_value = async_generator()
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS)
        )

        await self.run_diagnostic_settings_task(
            resource_cache=None,
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache=None,
        )

        # check the diagnostic setting was created
        self.create_or_update_setting.assert_awaited_once_with(
            resource_id1,
            DIAGNOSTIC_SETTING_NAME,
            AzureModelMatcher(
                {
                    "logs": [{"category": "cool_logs", "enabled": True}],
                    "storage_account_id": "/subscriptions/sub1/resourcegroups/lfo/providers/microsoft.storage/storageaccounts/ddlogstoragebc666ef914ec",
                }
            ),
        )

    async def test_task_leaves_existing_settings_unchanged(self):
        self.list_diagnostic_settings.return_value = async_generator(
            Mock(name=DIAGNOSTIC_SETTING_NAME, event_hub_name=TEST_EVENT_HUB_NAME)
        )
        self.list_diagnostic_settings_categories.return_value = async_generator()

        await self.run_diagnostic_settings_task(
            resource_cache=None,
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache=None,
        )
        self.create_or_update_setting.assert_not_awaited()
        self.delete_diagnostic_setting.assert_not_awaited()

    async def test_task_updates_incorrect_settings(self):
        self.list_diagnostic_settings.return_value = async_generator(
            mock(name=DIAGNOSTIC_SETTING_NAME, storage_account_id="wrong_storage_account_id", logs=None),
        )
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS)
        )

        await self.run_diagnostic_settings_task(
            resource_cache=None,
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache=None,
        )

        # check the diagnostic setting was created
        self.create_or_update_setting.assert_awaited_once_with(
            resource_id1,
            DIAGNOSTIC_SETTING_NAME,
            AzureModelMatcher(
                {
                    "logs": [{"category": "cool_logs", "enabled": True}],
                    "storage_account_id": "/subscriptions/sub1/resourcegroups/lfo/providers/microsoft.storage/storageaccounts/ddlogstoragebc666ef914ec",
                }
            ),
        )

    async def test_task_uses_already_found_settings_instead_of_requerying(self):
        self.list_diagnostic_settings.return_value = async_generator(
            mock(
                name=DIAGNOSTIC_SETTING_NAME,
                storage_account_id="wrong_storage_account_id",
                logs=[mock(category="cool_logs")],
            ),
        )
        self.list_diagnostic_settings_categories.side_effect = AssertionError("Should not be called")

        await self.run_diagnostic_settings_task(
            resource_cache=None,
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache=None,
        )

        # check the diagnostic setting was created
        self.create_or_update_setting.assert_awaited_once_with(
            resource_id1,
            DIAGNOSTIC_SETTING_NAME,
            AzureModelMatcher(
                {
                    "logs": [{"category": "cool_logs", "enabled": True}],
                    "storage_account_id": "/subscriptions/sub1/resourcegroups/lfo/providers/microsoft.storage/storageaccounts/ddlogstoragebc666ef914ec",
                }
            ),
        )

    async def test_resource_type_not_supported_skips_resource(self):
        http_error = HttpResponseError()
        http_error.error = Mock(code="ResourceTypeNotSupported")
        self.list_diagnostic_settings.return_value = async_generator(
            mock(
                name=DIAGNOSTIC_SETTING_NAME,
                storage_account_id="wrong_storage_account_id",
                logs=[mock(category="cool_logs")],
            ),
            http_error,
        )

        await self.run_diagnostic_settings_task(
            resource_cache=None,
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache={},
        )

        self.log.warning.assert_called_with("Resource type for %s unsupported, skipping", resource_id1)
        self.list_diagnostic_settings_categories.assert_not_called()
        self.create_or_update_setting.assert_not_awaited()

    async def test_max_diagnostic_settings_sends_event(self):
        self.list_diagnostic_settings.return_value = mock_diagnostic_settings(MAX_DIAGNOSTIC_SETTINGS)
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS),
        )
        event_cache = {
            sub_id1: {resource_id1: EventDict(diagnostic_settings_count=MAX_DIAGNOSTIC_SETTINGS, sent_event=False)}
        }

        task = await self.run_diagnostic_settings_task(
            resource_cache=None,
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache=event_cache,
        )

        self.create_or_update_setting.assert_not_awaited()
        self.send_max_settings_reached_event.assert_called_once_with(sub_id1, resource_id1)
        self.assertEqual(task.event_cache[sub_id1][resource_id1][DIAGNOSTIC_SETTINGS_COUNT], MAX_DIAGNOSTIC_SETTINGS)
        self.assertTrue(task.event_cache[sub_id1][resource_id1][SENT_EVENT])

    async def test_max_diagnostic_settings_avoid_duplicate_event(self):
        self.list_diagnostic_settings.return_value = mock_diagnostic_settings(MAX_DIAGNOSTIC_SETTINGS)
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS),
        )
        event_cache = {
            sub_id1: {resource_id1: EventDict(diagnostic_settings_count=MAX_DIAGNOSTIC_SETTINGS, sent_event=True)}
        }

        task = await self.run_diagnostic_settings_task(
            resource_cache=None,
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache=event_cache,
        )

        self.create_or_update_setting.assert_not_awaited()
        self.send_max_settings_reached_event.assert_not_called()
        self.assertEqual(task.event_cache[sub_id1][resource_id1][DIAGNOSTIC_SETTINGS_COUNT], MAX_DIAGNOSTIC_SETTINGS)
        self.assertTrue(task.event_cache[sub_id1][resource_id1][SENT_EVENT])

    async def test_max_diag_event_not_sent_on_empty(self):
        self.list_diagnostic_settings.return_value = async_generator()
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS),
        )
        event_cache = {sub_id1: {resource_id1: EventDict(diagnostic_settings_count=0, sent_event=False)}}

        task = await self.run_diagnostic_settings_task(
            resource_cache=None,
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache=event_cache,
        )

        self.create_or_update_setting.assert_awaited_once()
        self.send_max_settings_reached_event.assert_not_called()
        self.assertEqual(task.event_cache[sub_id1][resource_id1][DIAGNOSTIC_SETTINGS_COUNT], 1)
        self.assertFalse(task.event_cache[sub_id1][resource_id1][SENT_EVENT])

    async def test_max_diag_event_not_sent_on_existing_setting(self):
        self.list_diagnostic_settings.return_value = mock_diagnostic_settings(2)
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS),
        )
        event_cache = {sub_id1: {resource_id1: EventDict(diagnostic_settings_count=2, sent_event=False)}}

        task = await self.run_diagnostic_settings_task(
            resource_cache=None,
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache=event_cache,
        )

        self.create_or_update_setting.assert_awaited_once()
        self.send_max_settings_reached_event.assert_not_called()
        self.assertEqual(task.event_cache[sub_id1][resource_id1][DIAGNOSTIC_SETTINGS_COUNT], 3)
        self.assertFalse(task.event_cache[sub_id1][resource_id1][SENT_EVENT])

    async def test_failed_max_diag_event_does_not_update_cache(self):
        self.list_diagnostic_settings.return_value = mock_diagnostic_settings(MAX_DIAGNOSTIC_SETTINGS)
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS),
        )
        event_cache = {
            sub_id1: {resource_id1: EventDict(diagnostic_settings_count=MAX_DIAGNOSTIC_SETTINGS, sent_event=False)}
        }

        self.send_max_settings_reached_event.return_value = False

        task = await self.run_diagnostic_settings_task(
            resource_cache=None,
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache=event_cache,
        )

        self.create_or_update_setting.assert_not_awaited()
        self.send_max_settings_reached_event.assert_called_once_with(sub_id1, resource_id1)
        self.assertEqual(task.event_cache[sub_id1][resource_id1][DIAGNOSTIC_SETTINGS_COUNT], MAX_DIAGNOSTIC_SETTINGS)
        self.assertFalse(task.event_cache[sub_id1][resource_id1][SENT_EVENT])

    async def test_latest_resources_cache_schema_creates_setting(self):
        self.list_diagnostic_settings.return_value = async_generator()
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS)
        )

        await self.run_diagnostic_settings_task(
            resource_cache={
                sub_id1: {
                    region1: {
                        resource_id1: {"tags": ["filter:me"], "filtered_out": False},
                    }
                },
            },
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache={},
        )

        self.create_or_update_setting.assert_awaited_once_with(
            resource_id1,
            DIAGNOSTIC_SETTING_NAME,
            AzureModelMatcher(
                {
                    "logs": [{"category": "cool_logs", "enabled": True}],
                    "storage_account_id": "/subscriptions/sub1/resourcegroups/lfo/providers/microsoft.storage/storageaccounts/ddlogstoragebc666ef914ec",
                }
            ),
        )

    async def test_old_resources_cache_schema_creates_setting(self):
        self.list_diagnostic_settings.return_value = async_generator()
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS)
        )

        resource_cache = {
            sub_id1: {region1: {resource_id1}},
        }
        await self.run_diagnostic_settings_task(
            resource_cache=cast(ResourceCache, resource_cache),
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache={},
        )

        self.create_or_update_setting.assert_awaited_once_with(
            resource_id1,
            DIAGNOSTIC_SETTING_NAME,
            AzureModelMatcher(
                {
                    "logs": [{"category": "cool_logs", "enabled": True}],
                    "storage_account_id": "/subscriptions/sub1/resourcegroups/lfo/providers/microsoft.storage/storageaccounts/ddlogstoragebc666ef914ec",
                }
            ),
        )

    async def test_filtered_resource_skips_setting_create(self):
        self.list_diagnostic_settings.return_value = async_generator()
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS)
        )

        await self.run_diagnostic_settings_task(
            resource_cache={
                sub_id1: {
                    region1: {
                        resource_id1: {"tags": ["filter:me"], "filtered_out": True},
                    }
                },
            },
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache={},
        )

        self.create_or_update_setting.assert_not_awaited()

    async def test_filtered_resource_deletes_existing_setting(self):
        self.list_diagnostic_settings.return_value = async_generator(
            mock(name=DIAGNOSTIC_SETTING_NAME, storage_account_id=storage_account)
        )

        await self.run_diagnostic_settings_task(
            resource_cache={
                sub_id1: {
                    region1: {
                        resource_id1: {"tags": ["filter:me"], "filtered_out": True},
                    }
                },
            },
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache=None,
        )

        self.create_or_update_setting.assert_not_awaited()
        self.delete_diagnostic_setting.assert_awaited_once_with(resource_id1, DIAGNOSTIC_SETTING_NAME)
