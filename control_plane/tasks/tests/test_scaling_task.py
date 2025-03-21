# stdlib
from collections.abc import Callable
from datetime import datetime, timedelta
from json import dumps
from os import environ
from string import ascii_lowercase
from typing import Any, cast
from unittest import TestCase
from unittest.mock import ANY, Mock, call, patch

# 3rd party
from azure.mgmt.appcontainers.models import EnvironmentVar, Secret
from tenacity import RetryError

# project
from cache.assignment_cache import (
    ASSIGNMENT_CACHE_BLOB,
    AssignmentCache,
    deserialize_assignment_cache,
)
from cache.common import (
    STORAGE_ACCOUNT_TYPE,
    InvalidCacheError,
)
from cache.env import (
    CONFIG_ID_SETTING,
    CONNECTION_STRING_SECRET,
    CONTROL_PLANE_ID_SETTING,
    CONTROL_PLANE_REGION_SETTING,
    DD_API_KEY_SECRET,
    DD_API_KEY_SETTING,
    DD_SITE_SETTING,
    RESOURCE_GROUP_SETTING,
    SCALING_PERCENTAGE_SETTING,
    STORAGE_CONNECTION_SETTING,
)
from cache.metric_blob_cache import MetricBlobEntry
from cache.resources_cache import ResourceCache, ResourceMetadata
from tasks.scaling_task import (
    METRIC_COLLECTION_PERIOD_MINUTES,
    SCALING_METRIC_PERIOD_MINUTES,
    SCALING_TASK_NAME,
    ScalingTask,
    is_consistently_over_threshold,
    resources_to_move_by_load,
)
from tasks.tests.common import AsyncMockClient, AzureModelMatcher, TaskTestCase, mock

SUB_ID1 = "decc348e-ca9e-4925-b351-ae56b0d9f811"
EAST_US = "eastus"
EAST_US_2 = "eastus2"
WEST_US = "westus"
NEW_ZEALAND_NORTH = "newzealandnorth"
RG1 = "test_lfo"
CONTROL_PLANE_ID = "5a095f74c60a"


OLD_LOG_FORWARDER_ID = "5a095f74c60a"
NEW_LOG_FORWARDER_ID = "93a5885365f5"

resMetadata = ResourceMetadata(include=True)


def minutes_ago(minutes: float) -> float:
    """the unix timestamp for `minutes` ago"""
    return (datetime.now() - timedelta(minutes=minutes)).timestamp()


def generate_metrics(
    runtime: float | Callable[[int], float], resource_log_volume: dict[str, int], *, offset_mins: float = 0.5
) -> list[MetricBlobEntry]:
    """Generate a list of metric entries for a forwarder, starting at the offset
    time in minutes, and going back for the full period expected by the scaling task"""
    return [
        {
            "runtime_seconds": runtime(i) if callable(runtime) else runtime,
            "timestamp": minutes_ago(i + offset_mins),
            "resource_log_volume": resource_log_volume.copy(),
            "resource_log_bytes": {k: v * 10 for k, v in resource_log_volume.items()},
        }
        for i in range(METRIC_COLLECTION_PERIOD_MINUTES)
    ]


def generate_forwarder_ids(count: int) -> list[str]:
    return [f"{i}{ascii_lowercase[i] * 11}" for i in range(count)]


def collect_metrics_side_effect(
    mapping: dict[str, list[MetricBlobEntry]],
) -> Callable[[str, str, float], list[MetricBlobEntry]]:
    return lambda config_id, _region, _timestamp: mapping.get(config_id, [])


class TestScalingTask(TaskTestCase):
    TASK_NAME = SCALING_TASK_NAME
    maxDiff = 2000

    async def asyncSetUp(self) -> None:
        super().setUp()
        self.client = AsyncMockClient()
        self.patch_path("tasks.scaling_task.LogForwarderClient").return_value = self.client
        self.client.create_log_forwarder.return_value = STORAGE_ACCOUNT_TYPE
        self.client.create_log_forwarder_env.return_value = CONTROL_PLANE_ID
        self.forwarder_resources_mapping: dict[str, tuple[Mock | None, Mock | None]] = {
            OLD_LOG_FORWARDER_ID: (mock(), mock()),
            NEW_LOG_FORWARDER_ID: (mock(), mock()),
        }
        self.client.get_forwarder_resources.side_effect = self.forwarder_resources_mapping.get
        self.client.generate_forwarder_settings = Mock(
            side_effect=lambda config_id: [
                EnvironmentVar(name=STORAGE_CONNECTION_SETTING, secret_ref=CONNECTION_STRING_SECRET),
                EnvironmentVar(name=DD_API_KEY_SETTING, secret_ref=DD_API_KEY_SECRET),
                EnvironmentVar(name=DD_SITE_SETTING, value="datadoghq.com"),
                EnvironmentVar(name=CONTROL_PLANE_ID_SETTING, value=CONTROL_PLANE_ID),
                EnvironmentVar(name=CONFIG_ID_SETTING, value=config_id),
            ]
        )
        self.client.generate_forwarder_secrets.return_value = [
            Secret(name=DD_API_KEY_SECRET, value="some_api_key"),
            Secret(name=CONNECTION_STRING_SECRET, value="some_connection_string"),
        ]
        self.log = self.patch_path("tasks.task.log").getChild.return_value
        self.generate_unique_id = self.patch("generate_unique_id")
        p = patch.dict(
            environ,
            {
                RESOURCE_GROUP_SETTING: RG1,
                CONTROL_PLANE_ID_SETTING: CONTROL_PLANE_ID,
                CONTROL_PLANE_REGION_SETTING: EAST_US_2,
                SCALING_PERCENTAGE_SETTING: "0.7",
            },
        )
        p.start()
        self.addCleanup(p.stop)
        self.generate_unique_id.return_value = NEW_LOG_FORWARDER_ID

    @property
    def cache(self) -> AssignmentCache:
        return self.cache_value(ASSIGNMENT_CACHE_BLOB, deserialize_assignment_cache)

    async def run_scaling_task(self, resource_cache_state: ResourceCache, assignment_cache_state: AssignmentCache):
        async with ScalingTask(dumps(resource_cache_state, default=list), dumps(assignment_cache_state)) as task:
            await task.run()

    async def test_scaling_task_fails_without_valid_resource_cache(self):
        with self.assertRaises(InvalidCacheError):
            ScalingTask("invalid json", "{}")

    async def test_scaling_task_runs_with_old_resource_cache_schema(self):
        resource_cache_state = {SUB_ID1: {EAST_US: {"resource1", "resource2"}}}
        initial_assignment_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {"resource1": OLD_LOG_FORWARDER_ID},
                    "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                }
            }
        }

        await self.run_scaling_task(
            resource_cache_state=cast(ResourceCache, resource_cache_state),
            assignment_cache_state=initial_assignment_cache,
        )

        self.assertEqual(
            self.cache,
            {
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                }
            },
        )

    async def test_scaling_task_runs_with_latest_resource_cache_schema(self):
        resource_cache_state = {SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata}}}
        assignment_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {"resource1": OLD_LOG_FORWARDER_ID},
                    "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                }
            }
        }

        await self.run_scaling_task(
            resource_cache_state=cast(ResourceCache, resource_cache_state),
            assignment_cache_state=assignment_cache,
        )

        self.assertEqual(
            self.cache,
            {
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                }
            },
        )

    async def test_reset_invalid_scaling_cache(self):
        invalid_cache: Any = "not valid"
        await self.run_scaling_task(
            {},
            invalid_cache,
        )
        self.write_cache.assert_not_awaited()
        self.log.warning.assert_called_once_with("Assignment Cache is in an invalid format, task will reset the cache")

    async def test_new_regions_are_added_first_run(self):
        # GIVEN
        self.client.get_log_forwarder_managed_environment.return_value = False
        expected_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {},
                    "configurations": {},
                }
            }
        }

        # WHEN
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata}}},
            assignment_cache_state={},
        )

        # THEN
        self.client.create_log_forwarder.assert_not_awaited()
        self.client.create_log_forwarder_managed_environment.assert_called_once_with(EAST_US, wait=False)
        self.assertEqual(self.cache, expected_cache)

    async def test_new_regions_are_added_second_run(self):
        # GIVEN
        self.client.get_log_forwarder_managed_environment.return_value = True
        initial_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {},
                    "configurations": {},
                }
            }
        }
        expected_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {"resource1": NEW_LOG_FORWARDER_ID, "resource2": NEW_LOG_FORWARDER_ID},
                    "configurations": {NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                }
            }
        }

        # WHEN
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata}}},
            assignment_cache_state=initial_cache,
        )

        # THEN
        self.client.create_log_forwarder.assert_called_once_with(EAST_US, NEW_LOG_FORWARDER_ID)
        self.assertEqual(self.cache, expected_cache)

    async def test_failed_env_creation_triggers_delete(self):
        # GIVEN
        self.client.get_log_forwarder_managed_environment.return_value = False
        self.client.create_log_forwarder_managed_environment.side_effect = Exception("Test")
        initial_cache: AssignmentCache = {}

        # WHEN
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata}}},
            assignment_cache_state=initial_cache,
        )

        # THEN
        self.client.create_log_forwarder_managed_environment.assert_awaited_once_with(EAST_US, wait=False)
        self.client.delete_log_forwarder_env.assert_awaited_once_with(EAST_US, raise_error=False)

    async def test_errors_raised_when_forwarder_creation_fails(self):
        # GIVEN
        self.client.get_log_forwarder_managed_environment.return_value = True
        self.client.create_log_forwarder.side_effect = Exception("Test")
        expected_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {},
                    "configurations": {},
                }
            }
        }

        # WHEN
        with self.assertRaises(RetryError):
            await self.run_scaling_task(
                resource_cache_state={SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata}}},
                assignment_cache_state=expected_cache,
            )

        # THEN
        self.client.create_log_forwarder_env.assert_not_awaited()
        self.client.create_log_forwarder.assert_has_calls(
            [
                call(EAST_US, NEW_LOG_FORWARDER_ID),
                call(EAST_US, NEW_LOG_FORWARDER_ID),
                call(EAST_US, NEW_LOG_FORWARDER_ID),
            ]
        )
        self.client.delete_log_forwarder.assert_has_calls(
            [
                call(NEW_LOG_FORWARDER_ID, raise_error=False),
                call().__bool__(),
                call(NEW_LOG_FORWARDER_ID, raise_error=False),
                call().__bool__(),
                call(NEW_LOG_FORWARDER_ID, raise_error=False),
                call().__bool__(),
            ]
        )

    async def test_empty_regions_have_forwarders_deleted(self):
        # GIVEN
        expected_cache_state = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {},
                    "configurations": {},
                }
            },
        }

        # WHEN
        await self.run_scaling_task(
            resource_cache_state={},
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )

        # THEN
        self.client.create_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder.assert_awaited_once_with(OLD_LOG_FORWARDER_ID)
        self.assertEqual(self.cache, expected_cache_state)

    async def test_empty_regions_with_no_configs_have_forwarder_env_deleted(self):
        # GIVEN
        initial_cache_state: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {},
                    "configurations": {},
                }
            },
        }

        # WHEN
        await self.run_scaling_task(
            resource_cache_state={},
            assignment_cache_state=initial_cache_state,
        )

        # THEN
        self.client.create_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder_env.assert_awaited_once_with(EAST_US, raise_error=False)
        self.assertEqual(self.cache, {SUB_ID1: {}})

    async def test_regions_added_and_deleted(self):
        self.client.collect_forwarder_metrics.return_value = generate_metrics(runtime=2, resource_log_volume={})
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {WEST_US: {"resource3": resMetadata, "resource4": resMetadata}}},
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )

        self.client.create_log_forwarder.assert_called_once_with(WEST_US, NEW_LOG_FORWARDER_ID)
        self.client.delete_log_forwarder.assert_called_once_with(OLD_LOG_FORWARDER_ID)

        expected_cache: AssignmentCache = {
            SUB_ID1: {
                WEST_US: {
                    "resources": {"resource3": NEW_LOG_FORWARDER_ID, "resource4": NEW_LOG_FORWARDER_ID},
                    "configurations": {NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                },
                EAST_US: {
                    "resources": {},
                    "configurations": {},
                },
            }
        }
        self.assertEqual(self.cache, expected_cache)

    async def test_regions_to_be_cleaned_up_with_forwarder_metrics_arent_deleted(self):
        self.client.collect_forwarder_metrics.return_value = generate_metrics(
            runtime=2, resource_log_volume={"resource1": 100}
        )
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {WEST_US: {"resource3": resMetadata, "resource4": resMetadata}}},
            assignment_cache_state={
                SUB_ID1: {
                    WEST_US: {
                        "resources": {"resource3": NEW_LOG_FORWARDER_ID, "resource4": NEW_LOG_FORWARDER_ID},
                        "configurations": {NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    },
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    },
                }
            },
        )

        self.client.create_log_forwarder.assert_not_called()
        self.client.delete_log_forwarder.assert_not_called()

        expected_cache: AssignmentCache = {
            SUB_ID1: {
                WEST_US: {
                    "resources": {"resource3": NEW_LOG_FORWARDER_ID, "resource4": NEW_LOG_FORWARDER_ID},
                    "configurations": {NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                },
                EAST_US: {
                    "resources": {},
                    "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                },
            }
        }
        self.assertEqual(self.cache, expected_cache)

    async def test_container_app_env_in_control_plane_region_is_not_deleted_on_cleanup(self):
        non_control_plane_subscription_id = "some-other-subscription-id"
        await self.run_scaling_task(
            resource_cache_state={
                SUB_ID1: {WEST_US: {"resource1": resMetadata, "resource2": resMetadata}},
                non_control_plane_subscription_id: {},
            },
            assignment_cache_state={
                SUB_ID1: {
                    WEST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    },
                    EAST_US_2: {"configurations": {}, "resources": {}},  # to be cleaned up
                },
                non_control_plane_subscription_id: {
                    EAST_US_2: {"configurations": {}, "resources": {}},  # to be cleaned up
                },
            },
        )
        self.client.delete_log_forwarder_env.assert_not_called()
        self.assertEqual(
            self.cache,
            {
                SUB_ID1: {
                    WEST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
                non_control_plane_subscription_id: {},
            },
        )

    async def test_container_app_env_in_unsupported_region_is_not_deleted_on_cleanup(self):
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {WEST_US: {"resource1": resMetadata, "resource2": resMetadata}}},
            assignment_cache_state={
                SUB_ID1: {
                    WEST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    },
                    NEW_ZEALAND_NORTH: {"configurations": {}, "resources": {}},  # to be cleaned up
                },
            },
        )

        self.client.delete_log_forwarder_env.assert_not_called()
        self.assertEqual(
            self.cache,
            {
                SUB_ID1: {
                    WEST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                }
            },
        )

    async def test_resource_task_deleted_resources_are_deleted(self):
        await self.run_scaling_task(
            resource_cache_state={
                SUB_ID1: {
                    EAST_US: {"resource1": resMetadata},
                },
            },
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )

        self.client.create_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder.assert_not_awaited()

        self.assertEqual(
            self.cache,
            {
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )

    async def test_ensure_region_forwarders_deletes_region_when_all_forwarders_are_gone(self):
        self.forwarder_resources_mapping.clear()
        self.forwarder_resources_mapping[OLD_LOG_FORWARDER_ID] = (None, None)
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1": resMetadata}}},
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )
        self.client.get_forwarder_resources.assert_awaited_once_with(OLD_LOG_FORWARDER_ID)
        self.assertEqual(self.cache, {SUB_ID1: {}})

    async def test_ensure_region_forwarders_fixes_missing_settings(self):
        actual_settings = [
            EnvironmentVar(name=STORAGE_CONNECTION_SETTING, secret_ref=CONNECTION_STRING_SECRET),
            EnvironmentVar(name=DD_API_KEY_SETTING, secret_ref="messed-up-secret"),
            EnvironmentVar(name=DD_SITE_SETTING, value="newrelic.com"),  # oops!
            EnvironmentVar(name=CONTROL_PLANE_ID_SETTING, value=CONTROL_PLANE_ID),
            EnvironmentVar(name=CONFIG_ID_SETTING, value=OLD_LOG_FORWARDER_ID),
        ]
        actual_secrets = [
            Secret(name=DD_API_KEY_SECRET, value="invalid_api_key"),
            Secret(name=CONNECTION_STRING_SECRET, value="invalid_connection_string"),
        ]
        self.forwarder_resources_mapping.clear()
        self.forwarder_resources_mapping[OLD_LOG_FORWARDER_ID] = (
            mock(template=mock(containers=[mock(env=actual_settings)]), configuration=mock(secrets=actual_secrets)),
            mock(),
        )
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1": resMetadata}}},
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )
        self.client.get_forwarder_resources.assert_awaited_once_with(OLD_LOG_FORWARDER_ID)
        self.log.info.assert_any_call("Updating settings for forwarder %s", OLD_LOG_FORWARDER_ID)
        self.client.create_or_update_log_forwarder_container_app.assert_awaited_once_with(
            EAST_US,
            OLD_LOG_FORWARDER_ID,
            env=[
                AzureModelMatcher(dict(name=STORAGE_CONNECTION_SETTING, secret_ref=CONNECTION_STRING_SECRET)),
                AzureModelMatcher(dict(name=DD_API_KEY_SETTING, secret_ref=DD_API_KEY_SECRET)),
                AzureModelMatcher(dict(name=DD_SITE_SETTING, value="datadoghq.com")),
                AzureModelMatcher(dict(name=CONTROL_PLANE_ID_SETTING, value=CONTROL_PLANE_ID)),
                AzureModelMatcher(dict(name=CONFIG_ID_SETTING, value=OLD_LOG_FORWARDER_ID)),
            ],
            secrets=[
                AzureModelMatcher(dict(name=DD_API_KEY_SECRET, value="some_api_key")),
                AzureModelMatcher(dict(name=CONNECTION_STRING_SECRET, value="some_connection_string")),
            ],
        )

    async def test_ensure_region_forwarders_doesnt_make_api_calls_when_settings_are_correct(self):
        actual_settings = [
            EnvironmentVar(name=STORAGE_CONNECTION_SETTING, secret_ref=CONNECTION_STRING_SECRET),
            EnvironmentVar(name=DD_API_KEY_SETTING, secret_ref=DD_API_KEY_SECRET),
            EnvironmentVar(name=DD_SITE_SETTING, value="datadoghq.com"),
            EnvironmentVar(name=CONTROL_PLANE_ID_SETTING, value=CONTROL_PLANE_ID),
            EnvironmentVar(name=CONFIG_ID_SETTING, value=OLD_LOG_FORWARDER_ID),
        ]
        actual_secrets = [
            Secret(name=DD_API_KEY_SECRET, value="some_api_key"),
            Secret(name=CONNECTION_STRING_SECRET, value="some_connection_string"),
        ]
        self.forwarder_resources_mapping.clear()
        self.forwarder_resources_mapping[OLD_LOG_FORWARDER_ID] = (
            mock(template=mock(containers=[mock(env=actual_settings)]), configuration=mock(secrets=actual_secrets)),
            mock(),
        )
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1": resMetadata}}},
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                }
            },
        )
        self.client.get_forwarder_resources.assert_awaited_once_with(OLD_LOG_FORWARDER_ID)
        self.client.update_forwarder_settings.assert_not_awaited()

    async def test_log_forwarder_metrics_collected(self):
        self.client.collect_forwarder_metrics.return_value = generate_metrics(100, {"resource1": 4, "resource2": 6})

        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata}}},
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )

        self.client.collect_forwarder_metrics.assert_called_once_with(OLD_LOG_FORWARDER_ID, EAST_US, ANY)
        self.assertTrue(
            call("No valid metrics found for forwarder %s", OLD_LOG_FORWARDER_ID) not in self.log.warning.call_args_list
        )

    async def test_log_forwarders_scale_up_when_underscaled(self):
        self.client.collect_forwarder_metrics.return_value = generate_metrics(
            lambda i: 49.045 - (i * 0.2), {"resource1": 4000, "resource2": 6000}
        )

        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata}}},
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )

        self.client.create_log_forwarder.assert_awaited_once_with(EAST_US, NEW_LOG_FORWARDER_ID)
        self.client.delete_log_forwarder.assert_not_awaited()
        expected_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": NEW_LOG_FORWARDER_ID},
                    "configurations": {
                        OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                    },
                    "on_cooldown": True,
                }
            }
        }
        self.assertEqual(self.cache, expected_cache)

    async def test_log_forwarder_collected_with_old_metrics(self):
        self.client.collect_forwarder_metrics.return_value = generate_metrics(
            100, {"resource1": 4, "resource2": 6}, offset_mins=3
        )

        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata}}},
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )

        self.client.collect_forwarder_metrics.assert_called_once_with(OLD_LOG_FORWARDER_ID, EAST_US, ANY)
        self.assertTrue(
            call("No valid metrics found for forwarder %s", OLD_LOG_FORWARDER_ID) not in self.log.warning.call_args_list
        )

    async def test_log_forwarders_dont_scale_when_under_threshold(self):
        self.client.collect_forwarder_metrics.return_value = generate_metrics(
            22.2, {"resource1": 4000, "resource2": 6000}
        )
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata}}},
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )
        self.client.create_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder.assert_not_awaited()
        self.write_cache.assert_not_called()

    async def test_log_forwarders_dont_scale_when_at_max_capacity(self):
        forwarder_ids = generate_forwarder_ids(15)
        self.forwarder_resources_mapping.update({id: (mock(), mock()) for id in forwarder_ids})
        resource_log_volume = {f"resource{i}": i * 1000 for i in range(20)}
        self.client.collect_forwarder_metrics.return_value = generate_metrics(
            lambda i: 49.045 - (i * 0.2), resource_log_volume
        )

        assignment_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {
                        f"resource{i}": forwarder_ids[i] for i in range(15)
                    },  # assign 15 resources to 15 forwarders
                    "configurations": {forwarder_id: STORAGE_ACCOUNT_TYPE for forwarder_id in forwarder_ids},
                }
            },
        }

        # and assign more log-intensive resources to first forwarder to simulate scale-up context
        noisy_resources = {f"resource{i}": forwarder_ids[0] for i in range(15, 20)}
        assignment_cache[SUB_ID1][EAST_US]["resources"].update(noisy_resources)

        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {f"resource{i}": resMetadata for i in range(20)}}},
            assignment_cache_state=assignment_cache,
        )

        self.client.is_consistently_over_threshold.assert_not_awaited()
        self.client.create_log_forwarder.assert_not_awaited()

    async def test_new_resources_onboarded_during_scaling(self):
        self.client.collect_forwarder_metrics.return_value = generate_metrics(
            23, {"resource1": 4000, "resource2": 6000}
        )
        await self.run_scaling_task(
            resource_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resource1": resMetadata,
                        "resource2": resMetadata,
                        "resource3": resMetadata,
                        "resource4": resMetadata,
                    }
                }
            },
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )

        self.client.create_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder.assert_not_awaited()
        expected_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {
                        "resource1": OLD_LOG_FORWARDER_ID,
                        "resource2": OLD_LOG_FORWARDER_ID,
                        "resource3": OLD_LOG_FORWARDER_ID,
                        "resource4": OLD_LOG_FORWARDER_ID,
                    },
                    "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                }
            },
        }
        self.assertEqual(self.cache, expected_cache)

    async def test_new_resources_onboard_to_the_least_busy_forwarder(self):
        self.client.collect_forwarder_metrics.side_effect = collect_metrics_side_effect(
            {
                OLD_LOG_FORWARDER_ID: generate_metrics(2.5, {"resource1": 4000, "resource2": 6000}),
                NEW_LOG_FORWARDER_ID: generate_metrics(10.5, {"resource1": 4000, "resource2": 6000}),
            }
        )
        await self.run_scaling_task(
            resource_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resource1": resMetadata,
                        "resource2": resMetadata,
                        "resource3": resMetadata,
                        "resource4": resMetadata,
                    }
                }
            },
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": NEW_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                            NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                },
            },
        )

        self.client.create_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder.assert_not_awaited()

        expected_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {
                        "resource1": NEW_LOG_FORWARDER_ID,
                        "resource2": OLD_LOG_FORWARDER_ID,
                        "resource3": OLD_LOG_FORWARDER_ID,
                        "resource4": OLD_LOG_FORWARDER_ID,
                    },
                    "configurations": {
                        OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                    },
                }
            },
        }
        self.assertEqual(self.cache, expected_cache)

    async def test_deleted_resources_are_removed_before_scaling(self):
        self.client.collect_forwarder_metrics.return_value = generate_metrics(
            4000, {"resource1": 10, "resource2": 10, "resource3": 3000, "resource4": 5000}
        )
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata}}},
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {
                            "resource1": OLD_LOG_FORWARDER_ID,
                            "resource2": OLD_LOG_FORWARDER_ID,
                            "resource3": OLD_LOG_FORWARDER_ID,
                            "resource4": OLD_LOG_FORWARDER_ID,
                        },
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )

        self.client.create_log_forwarder.assert_called_once_with(EAST_US, NEW_LOG_FORWARDER_ID)
        self.client.delete_log_forwarder.assert_not_awaited()
        self.assertEqual(
            self.cache,
            {
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": NEW_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                            NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                        "on_cooldown": True,
                    }
                },
            },
        )

    async def test_scaling_up_based_on_resource_load_with_onboarding(self):
        self.client.collect_forwarder_metrics.return_value = generate_metrics(
            lambda i: 55 + i, {"resource1": 1000, "resource2": 10000, "resource3": 3000, "resource4": 50}
        )
        await self.run_scaling_task(
            resource_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resource1": resMetadata,
                        "resource2": resMetadata,
                        "resource3": resMetadata,
                        "resource4": resMetadata,
                        "resource5": resMetadata,
                        "resource6": resMetadata,
                    }
                }
            },
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {
                            "resource1": OLD_LOG_FORWARDER_ID,
                            "resource2": OLD_LOG_FORWARDER_ID,
                            "resource3": OLD_LOG_FORWARDER_ID,
                            "resource4": OLD_LOG_FORWARDER_ID,
                        },
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )

        self.client.create_log_forwarder.assert_awaited_once_with(EAST_US, NEW_LOG_FORWARDER_ID)
        self.client.delete_log_forwarder.assert_not_awaited()

        expected_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {
                        "resource1": OLD_LOG_FORWARDER_ID,
                        "resource2": NEW_LOG_FORWARDER_ID,
                        "resource3": OLD_LOG_FORWARDER_ID,
                        "resource4": OLD_LOG_FORWARDER_ID,
                        "resource5": OLD_LOG_FORWARDER_ID,
                        "resource6": OLD_LOG_FORWARDER_ID,
                    },
                    "configurations": {
                        OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                    },
                    "on_cooldown": True,
                }
            },
        }
        self.assertEqual(self.cache, expected_cache)

    async def test_scaling_up_with_only_one_resource(self):
        self.client.collect_forwarder_metrics.return_value = generate_metrics(58, {"resource1": 90000})
        initial_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {"resource1": OLD_LOG_FORWARDER_ID},
                    "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                }
            }
        }

        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1": resMetadata}}}, assignment_cache_state=initial_cache
        )
        self.log.warning.assert_called_once_with(
            "Forwarder %s only has one resource but is overwhelmed", OLD_LOG_FORWARDER_ID
        )

        self.client.create_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder.assert_not_awaited()

        self.write_cache.assert_not_awaited()

    async def test_orphaned_forwarders_are_cleaned_up(self):
        self.client.list_log_forwarder_ids.return_value = {OLD_LOG_FORWARDER_ID, NEW_LOG_FORWARDER_ID}
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata}}},
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )

        self.client.create_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder.assert_awaited_once_with(
            NEW_LOG_FORWARDER_ID, raise_error=False, max_attempts=1
        )
        self.write_cache.assert_not_awaited()

    async def test_no_resource_metrics_split_in_half(self):
        self.client.collect_forwarder_metrics.return_value = generate_metrics(57, {})
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata}}},
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )

        self.client.create_log_forwarder.assert_awaited_once_with(EAST_US, NEW_LOG_FORWARDER_ID)
        self.client.delete_log_forwarder.assert_not_awaited()

        expected_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {
                        "resource1": OLD_LOG_FORWARDER_ID,
                        "resource2": NEW_LOG_FORWARDER_ID,
                    },
                    "configurations": {
                        OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                    },
                    "on_cooldown": True,
                }
            },
        }
        self.assertEqual(self.cache, expected_cache)

    async def test_forwarder_without_resources_or_metrics_is_cleaned_up(self):
        self.client.collect_forwarder_metrics.side_effect = collect_metrics_side_effect(
            {
                OLD_LOG_FORWARDER_ID: generate_metrics(1.2, {"resource1": 1000, "resource2": 200, "resource3": 50}),
                NEW_LOG_FORWARDER_ID: generate_metrics(0.5, {}),
            }
        )

        await self.run_scaling_task(
            resource_cache_state={
                SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata, "resource3": resMetadata}}
            },
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {
                            "resource1": OLD_LOG_FORWARDER_ID,
                            "resource2": OLD_LOG_FORWARDER_ID,
                            "resource3": OLD_LOG_FORWARDER_ID,
                        },
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                            NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                },
            },
        )

        self.client.create_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder.assert_awaited_once_with(NEW_LOG_FORWARDER_ID)
        self.assertEqual(
            self.cache,
            {
                SUB_ID1: {
                    EAST_US: {
                        "resources": {
                            "resource1": OLD_LOG_FORWARDER_ID,
                            "resource2": OLD_LOG_FORWARDER_ID,
                            "resource3": OLD_LOG_FORWARDER_ID,
                        },
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )

    async def test_forwarder_without_resources_but_with_metrics_is_not_cleaned_up(self):
        self.client.collect_forwarder_metrics.side_effect = collect_metrics_side_effect(
            {
                OLD_LOG_FORWARDER_ID: generate_metrics(1.2, {"resource1": 1000, "resource2": 200}),
                NEW_LOG_FORWARDER_ID: generate_metrics(0.5, {"resource3": 100}),
            }
        )

        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata}}},
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                            NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                },
            },
        )

        self.client.create_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder.assert_not_awaited()
        self.write_cache.assert_not_awaited()

    async def test_two_phase_forwarder_cleanup(self):
        # Phase 1: Move resources to new forwarder
        self.client.collect_forwarder_metrics.side_effect = collect_metrics_side_effect(
            {
                OLD_LOG_FORWARDER_ID: generate_metrics(1.2, {"resource1": 1000}),
                NEW_LOG_FORWARDER_ID: generate_metrics(2.5, {"resource2": 2000}),
            }
        )
        resource_cache: ResourceCache = {SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata}}}
        await self.run_scaling_task(
            resource_cache_state=resource_cache,
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": NEW_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                            NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                },
            },
        )
        self.client.create_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder.assert_not_awaited()
        phase_1_cache = self.cache
        self.assertEqual(
            phase_1_cache,
            {
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                            NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                }
            },
        )
        # reset mocks
        self.write_cache.reset_mock()
        self.client.create_log_forwarder.reset_mock()
        self.client.delete_log_forwarder.reset_mock()

        # Phase 2: Clean up the old forwarder
        self.client.collect_forwarder_metrics.side_effect = collect_metrics_side_effect(
            {
                OLD_LOG_FORWARDER_ID: generate_metrics(1.2, {"resource1": 1000, "resource2": 2000}),
                NEW_LOG_FORWARDER_ID: generate_metrics(0.5, {}),  # resource2 is moved over
            }
        )

        await self.run_scaling_task(resource_cache_state=resource_cache, assignment_cache_state=phase_1_cache)
        self.client.create_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder.assert_called_once_with(NEW_LOG_FORWARDER_ID)
        self.assertEqual(
            self.cache,
            {
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                }
            },
        )

    async def test_forwarders_are_not_deleted_with_old_metrics(self):
        self.client.collect_forwarder_metrics.side_effect = collect_metrics_side_effect(
            {
                OLD_LOG_FORWARDER_ID: generate_metrics(1.2, {"resource1": 1000, "resource2": 200}),
                NEW_LOG_FORWARDER_ID: generate_metrics(
                    0.5, {"resource3": 100}, offset_mins=SCALING_METRIC_PERIOD_MINUTES + 1
                ),
            }
        )
        await self.run_scaling_task(
            resource_cache_state={
                SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata, "resource3": resMetadata}}
            },
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {
                            "resource1": OLD_LOG_FORWARDER_ID,
                            "resource2": OLD_LOG_FORWARDER_ID,
                            "resource3": OLD_LOG_FORWARDER_ID,
                        },
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                            NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                },
            },
        )
        self.client.create_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder.assert_not_awaited()
        self.write_cache.assert_not_awaited()

    async def test_forwarders_are_coalesced_not_deleted(self):
        self.client.collect_forwarder_metrics.side_effect = collect_metrics_side_effect(
            {
                OLD_LOG_FORWARDER_ID: generate_metrics(1.2, {"resource1": 1000, "resource2": 200, "resource3": 50}),
                NEW_LOG_FORWARDER_ID: generate_metrics(2.5, {"resource4": 4000}),
            }
        )

        await self.run_scaling_task(
            resource_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resource1": resMetadata,
                        "resource2": resMetadata,
                        "resource3": resMetadata,
                        "resource4": resMetadata,
                    }
                }
            },
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {
                            "resource1": OLD_LOG_FORWARDER_ID,
                            "resource2": OLD_LOG_FORWARDER_ID,
                            "resource3": OLD_LOG_FORWARDER_ID,
                            "resource4": NEW_LOG_FORWARDER_ID,
                        },
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                            NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                },
            },
        )

        self.client.create_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder.assert_not_awaited()
        self.assertEqual(
            self.cache,
            {
                SUB_ID1: {
                    EAST_US: {
                        "resources": {
                            "resource1": OLD_LOG_FORWARDER_ID,
                            "resource2": OLD_LOG_FORWARDER_ID,
                            "resource3": OLD_LOG_FORWARDER_ID,
                            "resource4": OLD_LOG_FORWARDER_ID,
                        },
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                            NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                }
            },
        )

    async def test_cooldown_period_for_scaling(self):
        # overwhelmed forwarder with 2 resources
        self.client.collect_forwarder_metrics.return_value = generate_metrics(
            100, {"resource1": 1000, "resource2": 2000}
        )
        resource_cache: ResourceCache = {SUB_ID1: {EAST_US: {"resource1": resMetadata, "resource2": resMetadata}}}
        await self.run_scaling_task(
            resource_cache_state=resource_cache,
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )
        # forwarder should be scaled up
        self.client.create_log_forwarder.assert_awaited_once_with(EAST_US, NEW_LOG_FORWARDER_ID)
        self.client.delete_log_forwarder.assert_not_awaited()
        new_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": NEW_LOG_FORWARDER_ID},
                    "configurations": {
                        OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                    },
                    "on_cooldown": True,
                }
            },
        }
        self.assertEqual(self.cache, new_cache)
        self.write_cache.reset_mock()
        self.client.create_log_forwarder.reset_mock()

        # same metrics as before
        await self.run_scaling_task(resource_cache_state=resource_cache, assignment_cache_state=new_cache)

        self.client.create_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder.assert_not_awaited()
        final_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": NEW_LOG_FORWARDER_ID},
                    "configurations": {
                        OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                    },
                }
            }
        }
        self.assertEqual(self.cache, final_cache)

    async def test_write_to_cache_partway_through(self):
        self.client.list_log_forwarder_ids.side_effect = ValueError("meow")
        with self.assertRaises(ValueError):
            await self.run_scaling_task(
                resource_cache_state={
                    SUB_ID1: {
                        EAST_US: {
                            "resource1": resMetadata,
                            "resource2": resMetadata,
                            "resource3": resMetadata,
                            "resource4": resMetadata,
                        }
                    }
                },
                assignment_cache_state={},
            )
        self.write_cache.assert_awaited()


class TestIsConsistentlyOverThreshold(TestCase):
    def test_metrics_over_threshold(self):
        self.assertTrue(
            is_consistently_over_threshold(
                metrics=[
                    {
                        "runtime_seconds": 24,
                        "timestamp": minutes_ago(4),
                        "resource_log_volume": {"resource1": 4200, "resource2": 6100},
                        "resource_log_bytes": {"resource1": 42000, "resource2": 61000},
                    },
                    {
                        "runtime_seconds": 28,
                        "timestamp": minutes_ago(3),
                        "resource_log_volume": {"resource1": 4300, "resource2": 6400},
                        "resource_log_bytes": {"resource1": 42000, "resource2": 61000},
                    },
                ],
                threshold=20,
                percentage=0.7,
            )
        )

    def test_no_metrics_not_over_threshold(self):
        self.assertFalse(is_consistently_over_threshold(metrics=[], threshold=0, percentage=0.7))

    def test_metrics_partially_over_threshold(self):
        self.assertFalse(
            is_consistently_over_threshold(
                metrics=[
                    {
                        "runtime_seconds": 43,
                        "timestamp": minutes_ago(5.5),
                        "resource_log_volume": {"resource1": 4000, "resource2": 6000},
                        "resource_log_bytes": {"resource1": 42000, "resource2": 61000},
                    },
                    {
                        "runtime_seconds": 44,
                        "timestamp": minutes_ago(4),
                        "resource_log_volume": {"resource1": 4200, "resource2": 6100},
                        "resource_log_bytes": {"resource1": 42000, "resource2": 61000},
                    },
                    {
                        "runtime_seconds": 48,
                        "timestamp": minutes_ago(3),
                        "resource_log_volume": {"resource1": 4300, "resource2": 6400},
                        "resource_log_bytes": {"resource1": 42000, "resource2": 61000},
                    },
                ],
                threshold=45,
                percentage=0.7,
            )
        )

    def test_metrics_partially_over_threshold_with_low_percentage_required(self):
        self.assertTrue(
            is_consistently_over_threshold(
                metrics=[
                    {
                        "runtime_seconds": 43,
                        "timestamp": minutes_ago(5.5),
                        "resource_log_volume": {"resource1": 4000, "resource2": 6000},
                        "resource_log_bytes": {"resource1": 42000, "resource2": 61000},
                    },
                    {
                        "runtime_seconds": 44,
                        "timestamp": minutes_ago(4),
                        "resource_log_volume": {"resource1": 4200, "resource2": 6100},
                        "resource_log_bytes": {"resource1": 42000, "resource2": 61000},
                    },
                    {
                        "runtime_seconds": 48,
                        "timestamp": minutes_ago(3),
                        "resource_log_volume": {"resource1": 4300, "resource2": 6400},
                        "resource_log_bytes": {"resource1": 42000, "resource2": 61000},
                    },
                ],
                threshold=45,
                percentage=0.2,
            )
        )

    def test_single_metric_over_threshold(self):
        self.assertTrue(
            is_consistently_over_threshold(
                metrics=[
                    {
                        "runtime_seconds": 46,
                        "timestamp": minutes_ago(3),
                        "resource_log_volume": {"resource1": 5670},
                        "resource_log_bytes": {"resource1": 42000},
                    },
                ],
                threshold=45,
                percentage=0.7,
            )
        )

    def test_single_metric_exactly_at_threshold(self):
        # granted it is extremely unlikely that we hit this edge case
        # but it is still a valid one to test
        self.assertFalse(
            is_consistently_over_threshold(
                metrics=[
                    {
                        "runtime_seconds": 45,
                        "timestamp": minutes_ago(3),
                        "resource_log_volume": {"resource1": 5600},
                        "resource_log_bytes": {"resource1": 42000},
                    },
                ],
                threshold=45,
                percentage=0.7,
            )
        )


class TestResourcesToMoveByLoad(TestCase):
    def test_exactly_half_load(self):
        self.assertEqual(
            list(
                resources_to_move_by_load(
                    {
                        "big_resource": 9001,
                        "resource1": 500,
                        "resource2": 1000,
                        "resource3": 500,
                        "resource4": 2000,
                        "resource5": 1,
                        "resource6": 3000,
                        "resource7": 2000,
                    }
                )
            ),
            ["big_resource"],
        )

    def test_two_resources(self):
        self.assertEqual(
            list(
                resources_to_move_by_load(
                    {
                        "resource1": 1,
                        "resource2": 9000,
                    }
                )
            ),
            ["resource2"],
        )

    def test_four_resources(self):
        self.assertEqual(
            list(
                resources_to_move_by_load(
                    {
                        "resource1": 4000,
                        "resource2": 6000,
                        "resource3": 5000,
                        "resource4": 7000,
                    }
                )
            ),
            ["resource2", "resource4"],
        )

    def test_three_resources(self):
        self.assertEqual(
            list(
                resources_to_move_by_load(
                    {
                        "resource1": 4000,
                        "resource2": 6000,
                        "resource3": 5000,
                    }
                )
            ),
            ["resource3", "resource2"],
        )

    def test_all_the_same_load_prefer_second_partition(self):
        self.assertEqual(
            list(
                resources_to_move_by_load(
                    {
                        "resource1": 5000,
                        "resource2": 5000,
                        "resource3": 5000,
                        "resource4": 5000,
                        "resource5": 5000,
                    }
                )
            ),
            ["resource3", "resource4", "resource5"],
        )

    def test_one_resource(self):
        # this isnt a real use case since we only call this function when we have
        # more than one resource, but it is worth adding a unit test regardless
        self.assertEqual(list(resources_to_move_by_load({"resource1": 5000})), ["resource1"])

    def test_no_resources(self):
        self.assertEqual(list(resources_to_move_by_load({})), [])
