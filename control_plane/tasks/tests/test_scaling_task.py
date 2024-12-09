# stdlib
from asyncio import sleep
from collections.abc import Callable
from datetime import datetime, timedelta
from json import dumps
from os import environ
from typing import Any
from unittest import TestCase
from unittest.mock import AsyncMock, Mock, call, patch

# 3rd party
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
from cache.metric_blob_cache import MetricBlobEntry
from cache.resources_cache import ResourceCache
from tasks.client.log_forwarder_client import LogForwarderClient
from tasks.scaling_task import (
    METRIC_COLLECTION_PERIOD_MINUTES,
    SCALING_METRIC_PERIOD_MINUTES,
    SCALING_TASK_NAME,
    ScalingTask,
    is_consistently_over_threshold,
    resources_to_move_by_load,
)
from tasks.tests.common import AsyncMockClient, TaskTestCase

SUB_ID1 = "decc348e-ca9e-4925-b351-ae56b0d9f811"
EAST_US = "eastus"
WEST_US = "westus"
RG1 = "test_lfo"
CONTROL_PLANE_ID = "5a095f74c60a"


OLD_LOG_FORWARDER_ID = "5a095f74c60a"
NEW_LOG_FORWARDER_ID = "93a5885365f5"


def minutes_ago(minutes: float) -> float:
    """the unix timestamp for `minutes` ago"""
    return (datetime.now() - timedelta(minutes=minutes)).timestamp()


def generate_metrics(
    runtime: float | Callable[[int], float], resource_log_volume: dict[str, int], *, offset_mins: float = 0.5
) -> list[MetricBlobEntry]:
    return [
        {
            "runtime_seconds": runtime(i) if callable(runtime) else runtime,
            "timestamp": minutes_ago(i + offset_mins),
            "resource_log_volume": resource_log_volume.copy(),
        }
        for i in range(METRIC_COLLECTION_PERIOD_MINUTES)
    ]


def collect_metrics_side_effect(
    mapping: dict[str, list[MetricBlobEntry]],
) -> Callable[[LogForwarderClient, str, float], list[MetricBlobEntry]]:
    return lambda _c, config_id, _t: mapping.get(config_id, [])


class TestScalingTask(TaskTestCase):
    TASK_NAME = SCALING_TASK_NAME
    maxDiff = 2000

    async def asyncSetUp(self) -> None:
        super().setUp()
        self.client = AsyncMockClient()
        self.patch_path("tasks.scaling_task.LogForwarderClient").return_value = self.client
        self.client.create_log_forwarder.return_value = STORAGE_ACCOUNT_TYPE
        self.client.create_log_forwarder_env.return_value = CONTROL_PLANE_ID

        self.log = self.patch("log")
        self.generate_unique_id = self.patch("generate_unique_id")
        p = patch.dict(environ, {"RESOURCE_GROUP": RG1, "CONTROL_PLANE_ID": CONTROL_PLANE_ID})
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
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2"}}},
            assignment_cache_state={},
        )

        # THEN
        self.client.create_log_forwarder.assert_not_awaited()
        self.client.create_log_forwarder_managed_environment.assert_called_once_with(EAST_US)
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
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2"}}},
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
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2"}}},
            assignment_cache_state=initial_cache,
        )

        # THEN
        self.client.create_log_forwarder_managed_environment.assert_awaited_once_with(EAST_US)
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

        expected_calls = [
            call(NEW_LOG_FORWARDER_ID, raise_error=False),
            call().__bool__(),
            call(NEW_LOG_FORWARDER_ID, raise_error=False),
            call().__bool__(),
            call(NEW_LOG_FORWARDER_ID, raise_error=False),
            call().__bool__(),
        ]

        # WHEN
        with self.assertRaises(RetryError):
            await self.run_scaling_task(
                resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2"}}},
                assignment_cache_state=expected_cache,
            )

        # THEN
        self.client.create_log_forwarder_env.assert_not_awaited()
        self.client.delete_log_forwarder.assert_has_calls(expected_calls)

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
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {WEST_US: {"resource1", "resource2"}}},
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
                    "resources": {"resource1": NEW_LOG_FORWARDER_ID, "resource2": NEW_LOG_FORWARDER_ID},
                    "configurations": {NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                },
                EAST_US: {
                    "resources": {},
                    "configurations": {},
                },
            }
        }
        self.assertEqual(self.cache, expected_cache)

    async def test_resource_task_deleted_resources_are_deleted(self):
        await self.run_scaling_task(
            resource_cache_state={
                SUB_ID1: {
                    EAST_US: {"resource1"},
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

    async def test_log_forwarder_metrics_collected(self):
        self.client.get_blob_metrics_lines.return_value = list(
            map(dumps, generate_metrics(100, {"resource1": 4, "resource2": 6}))
        )
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2"}}},
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )

        self.client.get_blob_metrics_lines.assert_called_once_with(OLD_LOG_FORWARDER_ID)
        self.assertTrue(
            call("No valid metrics found for forwarder %s", OLD_LOG_FORWARDER_ID) not in self.log.warning.call_args_list
        )

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    async def test_log_forwarders_scale_up_when_underscaled(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.return_value = generate_metrics(
            lambda i: 29.045 - (i * 0.2), {"resource1": 4000, "resource2": 6000}
        )

        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2"}}},
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
                }
            }
        }
        self.assertEqual(self.cache, expected_cache)

    async def test_log_forwarder_collected_with_old_metrics(self):
        self.client.get_blob_metrics_lines.return_value = list(
            map(dumps, generate_metrics(100, {"resource1": 4, "resource2": 6}, offset_mins=3))
        )

        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2"}}},
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )

        self.client.get_blob_metrics_lines.assert_called_once_with(OLD_LOG_FORWARDER_ID)
        self.assertTrue(
            call("No valid metrics found for forwarder %s", OLD_LOG_FORWARDER_ID) not in self.log.warning.call_args_list
        )

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    async def test_log_forwarders_dont_scale_when_under_threshold(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.return_value = generate_metrics(22.2, {"resource1": 4000, "resource2": 6000})
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2"}}},
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

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    async def test_new_resources_onboarded_during_scaling(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.return_value = generate_metrics(23, {"resource1": 4000, "resource2": 6000})
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2", "resource3", "resource4"}}},
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

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    async def test_new_resources_onboard_to_the_least_busy_forwarder(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.side_effect = collect_metrics_side_effect(
            {
                OLD_LOG_FORWARDER_ID: generate_metrics(2.5, {"resource1": 4000, "resource2": 6000}),
                NEW_LOG_FORWARDER_ID: generate_metrics(10.5, {"resource1": 4000, "resource2": 6000}),
            }
        )
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2", "resource3", "resource4"}}},
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

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    async def test_deleted_resources_are_removed_before_scaling(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.return_value = generate_metrics(
            4000, {"resource1": 10, "resource2": 10, "resource3": 3000, "resource4": 5000}
        )
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2"}}},
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
                    }
                },
            },
        )

    async def test_old_log_forwarder_metrics_not_collected(self):
        metrics = generate_metrics(
            100, {"resource1": 4, "resource2": 6}, offset_mins=METRIC_COLLECTION_PERIOD_MINUTES + 1
        )
        self.client.get_blob_metrics_lines.return_value = list(map(dumps, metrics))
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2"}}},
            assignment_cache_state={
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                },
            },
        )

        self.client.get_blob_metrics_lines.assert_called_once_with(OLD_LOG_FORWARDER_ID)
        self.log.warning.assert_called_with("No valid metrics found for forwarders in region %s", EAST_US)

    async def test_background_tasks_awaited(self):
        m = Mock()

        async def background_task():
            await sleep(0.05)
            m()

        async with ScalingTask(dumps({SUB_ID1: {}}), dumps({SUB_ID1: {}})) as task:
            for _ in range(3):
                task.submit_background_task(background_task())
            failing_task_error = Exception("test")
            task.submit_background_task(AsyncMock(side_effect=failing_task_error)())
            await task.run()

        self.assertEqual(m.call_count, 3)
        self.log.error.assert_called_once_with("Background task failed with an exception", exc_info=failing_task_error)

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    async def test_scaling_up_based_on_resource_load_with_onboarding(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.return_value = generate_metrics(
            lambda i: 35 + i, {"resource1": 1000, "resource2": 10000, "resource3": 3000, "resource4": 50}
        )
        await self.run_scaling_task(
            resource_cache_state={
                SUB_ID1: {EAST_US: {"resource1", "resource2", "resource3", "resource4", "resource5", "resource6"}}
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
                }
            },
        }
        self.assertEqual(self.cache, expected_cache)

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    async def test_scaling_up_with_only_one_resource(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.return_value = generate_metrics(38, {"resource1": 90000})
        initial_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {"resource1": OLD_LOG_FORWARDER_ID},
                    "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                }
            }
        }

        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1"}}}, assignment_cache_state=initial_cache
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
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2"}}},
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

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    async def test_no_resource_metrics_split_in_half(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.return_value = generate_metrics(37, {})
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2"}}},
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
                }
            },
        }
        self.assertEqual(self.cache, expected_cache)

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    async def test_forwarder_without_resources_or_metrics_is_cleaned_up(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.side_effect = collect_metrics_side_effect(
            {
                OLD_LOG_FORWARDER_ID: generate_metrics(1.2, {"resource1": 1000, "resource2": 200, "resource3": 50}),
                NEW_LOG_FORWARDER_ID: generate_metrics(0.5, {}),
            }
        )

        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2", "resource3"}}},
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

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    async def test_forwarder_without_resources_but_with_metrics_is_not_cleaned_up(
        self, collect_forwarder_metrics: AsyncMock
    ):
        collect_forwarder_metrics.side_effect = collect_metrics_side_effect(
            {
                OLD_LOG_FORWARDER_ID: generate_metrics(1.2, {"resource1": 1000, "resource2": 200}),
                NEW_LOG_FORWARDER_ID: generate_metrics(0.5, {"resource3": 100}),
            }
        )

        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2"}}},
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

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    async def test_two_phase_forwarder_cleanup(self, collect_forwarder_metrics: AsyncMock):
        # Phase 1: Move resources to new forwarder
        collect_forwarder_metrics.side_effect = collect_metrics_side_effect(
            {
                OLD_LOG_FORWARDER_ID: generate_metrics(1.2, {"resource1": 1000}),
                NEW_LOG_FORWARDER_ID: generate_metrics(2.5, {"resource2": 2000}),
            }
        )
        resource_cache = {SUB_ID1: {EAST_US: {"resource1", "resource2"}}}
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
        collect_forwarder_metrics.side_effect = collect_metrics_side_effect(
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

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    async def test_forwarders_are_not_deleted_with_old_metrics(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.side_effect = collect_metrics_side_effect(
            {
                OLD_LOG_FORWARDER_ID: generate_metrics(1.2, {"resource1": 1000, "resource2": 200}),
                NEW_LOG_FORWARDER_ID: generate_metrics(
                    0.5, {"resource3": 100}, offset_mins=SCALING_METRIC_PERIOD_MINUTES + 1
                ),
            }
        )
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2", "resource3"}}},
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

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    async def test_forwarders_are_coalesced_not_deleted(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.side_effect = collect_metrics_side_effect(
            {
                OLD_LOG_FORWARDER_ID: generate_metrics(1.2, {"resource1": 1000, "resource2": 200, "resource3": 50}),
                NEW_LOG_FORWARDER_ID: generate_metrics(2.5, {"resource4": 4000}),
            }
        )

        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2", "resource3", "resource4"}}},
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

    async def test_write_to_cache_partway_through(self):
        self.client.list_log_forwarder_ids.side_effect = ValueError("meow")
        with self.assertRaises(ValueError):
            await self.run_scaling_task(
                resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2", "resource3", "resource4"}}},
                assignment_cache_state={},
            )
        self.write_cache.assert_awaited()


class TestScalingTaskHelpers(TestCase):
    def test_metrics_over_threshold(self):
        self.assertTrue(
            is_consistently_over_threshold(
                metrics=[
                    {
                        "runtime_seconds": 24,
                        "timestamp": minutes_ago(4),
                        "resource_log_volume": {"resource1": 4200, "resource2": 6100},
                    },
                    {
                        "runtime_seconds": 28,
                        "timestamp": minutes_ago(3),
                        "resource_log_volume": {"resource1": 4300, "resource2": 6400},
                    },
                ],
                threshold=20,
            )
        )

    def test_no_metrics_not_over_threshold(self):
        self.assertFalse(is_consistently_over_threshold(metrics=[], threshold=0))

    def test_metrics_partially_over_threshold(self):
        self.assertFalse(
            is_consistently_over_threshold(
                metrics=[
                    {
                        "runtime_seconds": 23,
                        "timestamp": minutes_ago(5.5),
                        "resource_log_volume": {"resource1": 4000, "resource2": 6000},
                    },
                    {
                        "runtime_seconds": 24,
                        "timestamp": minutes_ago(4),
                        "resource_log_volume": {"resource1": 4200, "resource2": 6100},
                    },
                    {
                        "runtime_seconds": 28,
                        "timestamp": minutes_ago(3),
                        "resource_log_volume": {"resource1": 4300, "resource2": 6400},
                    },
                ],
                threshold=25,
            )
        )

    def test_single_metric_over_threshold(self):
        self.assertTrue(
            is_consistently_over_threshold(
                metrics=[
                    {"runtime_seconds": 26, "timestamp": minutes_ago(3), "resource_log_volume": {"resource1": 5670}},
                ],
                threshold=25,
            )
        )

    def test_single_metric_exactly_at_threshold(self):
        # granted it is extremely unlikely that we hit this edge case
        # but it is still a valid one to test
        self.assertFalse(
            is_consistently_over_threshold(
                metrics=[
                    {"runtime_seconds": 25, "timestamp": minutes_ago(3), "resource_log_volume": {"resource1": 5600}},
                ],
                threshold=25,
            )
        )

    def test_resources_to_move_by_load_exactly_half_load(self):
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

    def test_resources_to_move_by_load_two_resources(self):
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

    def test_resources_to_move_by_load_four_resources(self):
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

    def test_resources_to_move_by_load_three_resources(self):
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

    def test_resources_to_move_by_load_all_the_same_load_prefer_second_partition(self):
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

    def test_resources_to_move_by_load_one_resource(self):
        # this isnt a real use case since we only call this function when we have
        # more than one resource, but it is worth adding a unit test regardless
        self.assertEqual(list(resources_to_move_by_load({"resource1": 5000})), ["resource1"])

    def test_resources_to_move_by_load_no_resources(self):
        self.assertEqual(list(resources_to_move_by_load({})), [])
