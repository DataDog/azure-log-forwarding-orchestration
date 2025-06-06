# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from collections.abc import AsyncIterable
from json import dumps
from os import environ
from typing import Final
from unittest.mock import AsyncMock, Mock, call, patch
from uuid import uuid4

# 3p
from azure.core.exceptions import HttpResponseError
from azure.mgmt.monitor.models import CategoryType

# project
from cache.assignment_cache import AssignmentCache
from cache.common import STORAGE_ACCOUNT_TYPE, InvalidCacheError
from cache.diagnostic_settings_cache import DIAGNOSTIC_SETTINGS_COUNT, SENT_EVENT, DiagnosticSettingsCache, EventDict
from cache.env import CONTROL_PLANE_ID_SETTING
from cache.resources_cache import INCLUDE_KEY, ResourceCache, ResourceCacheV1
from cache.tests import TEST_EVENT_HUB_NAME
from tasks.client.datadog_api_client import StatusCode
from tasks.diagnostic_settings_task import (
    DIAGNOSTIC_SETTING_PREFIX,
    DIAGNOSTIC_SETTINGS_TASK_NAME,
    MAX_DIAGNOSTIC_SETTINGS,
    DiagnosticSettingsTask,
)
from tasks.tests.common import AsyncMockClient, AzureModelMatcher, TaskTestCase, async_generator, mock
from tasks.version import VERSION

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
        self.datadog_client = AsyncMockClient()
        self.status_client = AsyncMockClient()
        self.datadog_client.submit_status_update = self.status_client
        self.patch_path("tasks.task.DatadogClient").return_value = self.datadog_client
        self.uuid = str(uuid4())
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
        env_dict = {"RESOURCE_GROUP": "lfo", "CONTROL_PLANE_ID": control_plane_id}
        env = patch.dict(environ, env_dict)
        env.start()
        self.addCleanup(env.stop)
        self.env.update(env_dict)

    async def run_diagnostic_settings_task(
        self,
        resource_cache: ResourceCache | ResourceCacheV1 | None,
        assignment_cache: AssignmentCache,
        event_cache: DiagnosticSettingsCache | None,
        execution_id: str = "",
        is_initial_run: bool = False,
    ) -> DiagnosticSettingsTask:
        async with DiagnosticSettingsTask(
            dumps(resource_cache, default=list),
            dumps(assignment_cache),
            dumps(event_cache),
            execution_id=execution_id,
            is_initial_run=is_initial_run,
        ) as task:
            await task.run()
        return task

    def test_malformed_resources_cache_errors_in_constructor(self):
        with self.assertRaises(InvalidCacheError) as e:
            DiagnosticSettingsTask("malformed", "", "")
        self.assertEqual(
            str(e.exception), "Resource Cache is in an invalid format, failing this task until it is valid"
        )

    def test_malformed_assignment_cache_errors_in_constructor(self):
        with self.assertRaises(InvalidCacheError) as e:
            DiagnosticSettingsTask("{}", "malformed", "")
        self.assertEqual(
            str(e.exception), "Assignment Cache is in an invalid format, failing this task until it is valid"
        )

    async def test_task_adds_missing_settings(self):
        self.list_diagnostic_settings.return_value = async_generator()
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS)
        )

        await self.run_diagnostic_settings_task(
            resource_cache={},
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
            resource_cache={},
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
            resource_cache={},
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
            resource_cache={},
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
            resource_cache={},
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

        task = await self.run_diagnostic_settings_task(
            resource_cache={},
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache={
                sub_id1: {resource_id1: EventDict(diagnostic_settings_count=MAX_DIAGNOSTIC_SETTINGS, sent_event=False)}
            },
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

        task = await self.run_diagnostic_settings_task(
            resource_cache={},
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache={
                sub_id1: {resource_id1: EventDict(diagnostic_settings_count=MAX_DIAGNOSTIC_SETTINGS, sent_event=True)}
            },
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

        task = await self.run_diagnostic_settings_task(
            resource_cache={},
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache={sub_id1: {resource_id1: EventDict(diagnostic_settings_count=0, sent_event=False)}},
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

        task = await self.run_diagnostic_settings_task(
            resource_cache={},
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache={sub_id1: {resource_id1: EventDict(diagnostic_settings_count=2, sent_event=False)}},
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

        self.send_max_settings_reached_event.return_value = False

        task = await self.run_diagnostic_settings_task(
            resource_cache={},
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache={
                sub_id1: {resource_id1: EventDict(diagnostic_settings_count=MAX_DIAGNOSTIC_SETTINGS, sent_event=False)}
            },
        )

        self.create_or_update_setting.assert_not_awaited()
        self.send_max_settings_reached_event.assert_called_once_with(sub_id1, resource_id1)
        self.assertEqual(task.event_cache[sub_id1][resource_id1][DIAGNOSTIC_SETTINGS_COUNT], MAX_DIAGNOSTIC_SETTINGS)
        self.assertFalse(task.event_cache[sub_id1][resource_id1][SENT_EVENT])

    async def test_v2_resources_cache_schema_creates_setting(self):
        self.list_diagnostic_settings.return_value = async_generator()
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS)
        )

        await self.run_diagnostic_settings_task(
            resource_cache={sub_id1: {region1: {resource_id1: {INCLUDE_KEY: True}}}},
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

    async def test_v1_resources_cache_schema_creates_setting(self):
        self.list_diagnostic_settings.return_value = async_generator()
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS)
        )

        await self.run_diagnostic_settings_task(
            resource_cache={sub_id1: {region1: {resource_id1}}},
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

    async def test_filtered_out_resource_skips_setting_create(self):
        self.list_diagnostic_settings.return_value = async_generator()
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS)
        )

        await self.run_diagnostic_settings_task(
            resource_cache={
                sub_id1: {
                    region1: {
                        resource_id1: {INCLUDE_KEY: False},
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

    async def test_filtered_out_resource_deletes_existing_setting(self):
        self.list_diagnostic_settings.return_value = async_generator(
            mock(name=DIAGNOSTIC_SETTING_NAME, storage_account_id=storage_account)
        )

        task = await self.run_diagnostic_settings_task(
            resource_cache={
                sub_id1: {
                    region1: {
                        resource_id1: {INCLUDE_KEY: False},
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
            event_cache={sub_id1: {resource_id1: EventDict(diagnostic_settings_count=2, sent_event=False)}},
        )

        self.create_or_update_setting.assert_not_awaited()
        self.delete_diagnostic_setting.assert_awaited_once_with(resource_id1, DIAGNOSTIC_SETTING_NAME)
        self.assertEqual(len(task.event_cache[sub_id1]), 0)

    async def test_filtered_out_resource_delete_fails(self):
        self.list_diagnostic_settings.return_value = async_generator(
            mock(name=DIAGNOSTIC_SETTING_NAME, storage_account_id=storage_account)
        )

        self.delete_diagnostic_setting.side_effect = HttpResponseError("503 - service unavailable")

        task = await self.run_diagnostic_settings_task(
            resource_cache={
                sub_id1: {region1: {resource_id1: {INCLUDE_KEY: False}}},
            },
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache={sub_id1: {resource_id1: EventDict(diagnostic_settings_count=2, sent_event=False)}},
        )

        self.delete_diagnostic_setting.assert_awaited_once_with(resource_id1, DIAGNOSTIC_SETTING_NAME)
        self.log.exception.assert_called_with("Failed to delete diagnostic setting for resource %s", resource_id1)
        self.assertIsNotNone(task.event_cache[sub_id1][resource_id1])
        self.assertEqual(task.event_cache[sub_id1][resource_id1][DIAGNOSTIC_SETTINGS_COUNT], 2)

    async def test_tags(self):
        self.env[CONTROL_PLANE_ID_SETTING] = "a2b4c5d6"

        task = await self.run_diagnostic_settings_task(
            resource_cache={},
            assignment_cache={},
            event_cache={},
        )

        self.assertCountEqual(
            task.tags,
            [
                "forwarder:lfocontrolplane",
                "task:diagnostic_settings_task",
                "control_plane_id:a2b4c5d6",
                f"version:{VERSION}",
            ],
        )

    async def test_task_initial_run_golden_path(self):
        self.list_diagnostic_settings.return_value = async_generator()
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS)
        )

        await self.run_diagnostic_settings_task(
            resource_cache={},
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache=None,
            execution_id=self.uuid,
            is_initial_run=True,
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

        expected_calls = [
            call(
                "diagnostic_settings_task.task_start",
                StatusCode.OK,
                "Diagnostic settings task started",
                self.uuid,
                "unknown",
                control_plane_id,
            ),
            call(
                "diagnostic_settings_task.task_complete",
                StatusCode.OK,
                "Diagnostic settings task completed",
                self.uuid,
                "unknown",
                control_plane_id,
            ),
        ]

        self.status_client.assert_has_calls(expected_calls)
        self.assertEqual(self.status_client.call_count, len(expected_calls))

    async def test_task_initial_run_failure_to_get_settings(self):
        test_string = "meow"

        def list_diagnostic_settings(resource_id):
            raise HttpResponseError(test_string)

        self.list_diagnostic_settings.side_effect = list_diagnostic_settings

        await self.run_diagnostic_settings_task(
            resource_cache={},
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache=None,
            execution_id=self.uuid,
            is_initial_run=True,
        )

        expected_calls = [
            call(
                "diagnostic_settings_task.task_start",
                StatusCode.OK,
                "Diagnostic settings task started",
                self.uuid,
                "unknown",
                control_plane_id,
            ),
            call(
                "diagnostic_settings_task.process_resource",
                StatusCode.AZURE_RESPONSE_ERROR,
                f"Failed to get diagnostic settings for resource {resource_id1}",
                self.uuid,
                "unknown",
                control_plane_id,
            ),
            call(
                "diagnostic_settings_task.task_complete",
                StatusCode.OK,
                "Diagnostic settings task completed",
                self.uuid,
                "unknown",
                control_plane_id,
            ),
        ]

        self.status_client.assert_has_calls(expected_calls)
        self.assertEqual(self.status_client.call_count, len(expected_calls))

    async def test_task_initial_run_failure_to_create_setting_with_http_error(self):
        test_string = "meow"

        def create_or_update_diagnostic_setting(resource_id, name, settings):
            raise HttpResponseError(test_string)

        self.create_or_update_setting.side_effect = create_or_update_diagnostic_setting

        self.list_diagnostic_settings.return_value = async_generator()
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS)
        )

        await self.run_diagnostic_settings_task(
            resource_cache={},
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache=None,
            execution_id=self.uuid,
            is_initial_run=True,
        )

        expected_calls = [
            call(
                "diagnostic_settings_task.task_start",
                StatusCode.OK,
                "Diagnostic settings task started",
                self.uuid,
                "unknown",
                control_plane_id,
            ),
            call(
                "diagnostic_settings_task.create_or_update_diagnostic_setting",
                StatusCode.RESOURCE_CREATION_ERROR,
                f"Failed to create or update diagnostic setting for resource {resource_id1} Reason: {test_string}",
                self.uuid,
                "unknown",
                control_plane_id,
            ),
            call(
                "diagnostic_settings_task.task_complete",
                StatusCode.OK,
                "Diagnostic settings task completed",
                self.uuid,
                "unknown",
                control_plane_id,
            ),
        ]

        self.status_client.assert_has_calls(expected_calls)
        self.assertEqual(self.status_client.call_count, len(expected_calls))

    async def test_task_initial_run_failure_to_create_setting_with_unknown_error(self):
        test_string = "meow"

        def create_or_update_diagnostic_setting(resource_id, name, settings):
            raise ValueError(test_string)

        self.create_or_update_setting.side_effect = create_or_update_diagnostic_setting

        self.list_diagnostic_settings.return_value = async_generator()
        self.list_diagnostic_settings_categories.return_value = async_generator(
            mock(name="cool_logs", category_type=CategoryType.LOGS)
        )

        await self.run_diagnostic_settings_task(
            resource_cache={},
            assignment_cache={
                sub_id1: {
                    region1: {
                        "configurations": {config_id1: STORAGE_ACCOUNT_TYPE},
                        "resources": {resource_id1: config_id1},
                    }
                }
            },
            event_cache=None,
            execution_id=self.uuid,
            is_initial_run=True,
        )

        expected_calls = [
            call(
                "diagnostic_settings_task.task_start",
                StatusCode.OK,
                "Diagnostic settings task started",
                self.uuid,
                "unknown",
                control_plane_id,
            ),
            call(
                "diagnostic_settings_task.create_or_update_diagnostic_setting",
                StatusCode.UNKNOWN_ERROR,
                f"Failed to create or update diagnostic setting for resource {resource_id1} Reason: {test_string}",
                self.uuid,
                "unknown",
                control_plane_id,
            ),
            call(
                "diagnostic_settings_task.task_complete",
                StatusCode.OK,
                "Diagnostic settings task completed",
                self.uuid,
                "unknown",
                control_plane_id,
            ),
        ]

        self.status_client.assert_has_calls(expected_calls)
        self.assertEqual(self.status_client.call_count, len(expected_calls))
