# stdlib
from asyncio import sleep
from datetime import datetime, timedelta
from json import dumps
from os import environ
from typing import Any
from unittest import TestCase
from unittest.mock import AsyncMock, Mock, call, patch

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
    SCALING_TASK_NAME,
    ScalingTask,
    is_consistently_over_threshold,
    resources_to_move_by_load,
)
from tasks.tests.common import AsyncMockClient, TaskTestCase, UnexpectedException

SUB_ID1 = "decc348e-ca9e-4925-b351-ae56b0d9f811"
EAST_US = "eastus"
WEST_US = "westus"
RG1 = "test_lfo"


OLD_LOG_FORWARDER_ID = "5a095f74c60a"
NEW_LOG_FORWARDER_ID = "93a5885365f5"


def minutes_ago(minutes: float) -> float:
    """the unix timestamp for `minutes` ago"""
    return (datetime.now() - timedelta(minutes=minutes)).timestamp()


def generate_metrics(runtime: float, resource_log_volume: dict[str, int]) -> list[MetricBlobEntry]:
    return [
        {
            "runtime_seconds": runtime,
            "timestamp": minutes_ago((i * 0.5) + 0.5),
            "resource_log_volume": resource_log_volume.copy(),
        }
        for i in range(3)
    ]


class TestScalingTask(TaskTestCase):
    TASK_NAME = SCALING_TASK_NAME
    maxDiff = 2000

    async def asyncSetUp(self) -> None:
        super().setUp()
        self.client = AsyncMockClient()
        self.patch_path("tasks.scaling_task.LogForwarderClient").return_value = self.client
        self.client.create_log_forwarder.return_value = STORAGE_ACCOUNT_TYPE

        self.log = self.patch("log")
        self.generate_unique_id = self.patch("generate_unique_id")
        p = patch.dict(environ, {"RESOURCE_GROUP": RG1})
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

    async def test_new_regions_are_added(self):
        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1", "resource2"}}},
            assignment_cache_state={},
        )

        self.client.create_log_forwarder.assert_called_once_with(EAST_US, NEW_LOG_FORWARDER_ID)
        expected_cache: AssignmentCache = {
            SUB_ID1: {
                EAST_US: {
                    "resources": {"resource1": NEW_LOG_FORWARDER_ID, "resource2": NEW_LOG_FORWARDER_ID},
                    "configurations": {NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                }
            }
        }
        self.assertEqual(self.cache, expected_cache)

    async def test_gone_regions_are_deleted(self):
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

        self.client.create_log_forwarder.assert_not_awaited()
        self.client.delete_log_forwarder.assert_awaited_once_with(OLD_LOG_FORWARDER_ID)

        self.assertEqual(self.cache, {})

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
                }
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
        current_time = (datetime.now()).timestamp()
        self.client.get_blob_metrics_lines.return_value = [
            dumps(
                {
                    "timestamp": current_time,
                    "runtime_seconds": 211,
                    "resource_log_volume": {"resource1": 4, "resource2": 6},
                }
            ),
            dumps(
                {
                    "timestamp": current_time,
                    "runtime_seconds": 199,
                    "resource_log_volume": {"resource1": 4, "resource2": 6},
                }
            ),
        ]
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
        collect_forwarder_metrics.return_value = [
            {
                "runtime_seconds": 29.045 - (i * 0.2),
                "timestamp": (datetime.now() - timedelta(seconds=30 * i)).timestamp(),
                "resource_log_volume": {"resource1": 4000, "resource2": 6000},
            }
            for i in range(6)
        ]

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
        old_time = (datetime.now() - timedelta(minutes=(METRIC_COLLECTION_PERIOD_MINUTES + 1))).timestamp()
        current_time = (datetime.now()).timestamp()
        self.client.get_blob_metrics_lines.return_value = [
            dumps(
                {
                    "timestamp": current_time,
                    "runtime_seconds": 211,
                    "resource_log_volume": {"resource1": 4000, "resource2": 6000},
                }
            ),
            dumps(
                {
                    "timestamp": old_time,
                    "runtime_seconds": 199,
                    "resource_log_volume": {"resource1": 4000, "resource2": 6000},
                }
            ),
        ]

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
    async def test_log_forwarders_dont_scale_when_not_needed(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.return_value = [
            {
                "runtime_seconds": 22.2,
                "timestamp": (datetime.now() - timedelta(seconds=30 * i)).timestamp(),
                "resource_log_volume": {"resource1": 4000, "resource2": 6000},
            }
            for i in range(60)
        ]
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
        collect_forwarder_metrics.return_value = [
            {
                "runtime_seconds": 23,
                "timestamp": (datetime.now() - timedelta(seconds=30 * i)).timestamp(),
                "resource_log_volume": {"resource1": 4000, "resource2": 6000},
            }
            for i in range(60)
        ]
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
        forwarder_runtimes = {
            OLD_LOG_FORWARDER_ID: 2.5,
            NEW_LOG_FORWARDER_ID: 10.5,
        }

        def metrics_side_effect(_client: LogForwarderClient, config_id: str, _old_ts: float) -> list[MetricBlobEntry]:
            return [
                {
                    "runtime_seconds": forwarder_runtimes[config_id],
                    "timestamp": (datetime.now() - timedelta(seconds=30 * i)).timestamp(),
                    "resource_log_volume": {"resource1": 4000, "resource2": 6000},
                }
                for i in range(60)
            ]

        collect_forwarder_metrics.side_effect = metrics_side_effect
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
        collect_forwarder_metrics.return_value = [
            {
                "runtime_seconds": 4000,
                "timestamp": minutes_ago(0.5),
                "resource_log_volume": {"resource1": 10, "resource2": 10, "resource3": 3000, "resource4": 5000},
            }
        ]
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
        old_time = (datetime.now() - timedelta(minutes=(METRIC_COLLECTION_PERIOD_MINUTES + 1))).timestamp()
        self.client.get_blob_metrics_lines.return_value = [
            dumps(
                {
                    "timestamp": old_time,
                    "runtime_seconds": 211,
                    "resource_log_volume": {"resource1": 4000, "resource2": 6000},
                }
            ),
            dumps(
                {
                    "timestamp": old_time,
                    "runtime_seconds": 199,
                    "resource_log_volume": {"resource1": 4000, "resource2": 6000},
                }
            ),
        ]
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

    async def test_unexpected_failure_skips_cache_write(self):
        self.client.get_blob_metrics_lines.side_effect = UnexpectedException("unexpected")
        write_caches = self.patch("ScalingTask.write_caches")
        with self.assertRaises(UnexpectedException):
            await self.run_scaling_task(
                {SUB_ID1: {EAST_US: {"resource1", "resource2"}, WEST_US: {"resource3"}}},
                {
                    SUB_ID1: {
                        EAST_US: {
                            "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                            "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                        }
                    },
                },
            )
        write_caches.assert_not_awaited()

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    async def test_scaling_up_based_on_resource_load_with_onboarding(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.return_value = [
            {
                "runtime_seconds": 40,
                "timestamp": minutes_ago(1),
                "resource_log_volume": {"resource1": 1000, "resource2": 10000, "resource3": 3000, "resource4": 50},
            },
            {
                "runtime_seconds": 33,
                "timestamp": minutes_ago(1.5),
                "resource_log_volume": {"resource1": 1100, "resource2": 9000, "resource3": 2000, "resource4": 100},
            },
            {
                "runtime_seconds": 42.2,
                "timestamp": minutes_ago(2),
                "resource_log_volume": {"resource1": 900, "resource2": 12000, "resource3": 3300, "resource4": 3},
            },
        ]
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
        collect_forwarder_metrics.return_value = [
            {
                "runtime_seconds": 35,
                "timestamp": minutes_ago(1.5),
                "resource_log_volume": {"resource1": 90000},
            },
            {
                "runtime_seconds": 40,
                "timestamp": minutes_ago(1),
                "resource_log_volume": {"resource1": 100000},
            },
            {
                "runtime_seconds": 39.1,
                "timestamp": minutes_ago(0.5),
                "resource_log_volume": {"resource1": 95000},
            },
        ]
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
        collect_forwarder_metrics.return_value = [
            {
                "runtime_seconds": 35,
                "timestamp": minutes_ago(1.5),
                "resource_log_volume": {},
            },
            {
                "runtime_seconds": 40,
                "timestamp": minutes_ago(1),
                "resource_log_volume": {},
            },
            {
                "runtime_seconds": 39.1,
                "timestamp": minutes_ago(0.5),
                "resource_log_volume": {},
            },
        ]
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
    async def test_forwarder_without_resources_is_cleaned_up(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.side_effect = lambda _c, config_id, _t: {
            OLD_LOG_FORWARDER_ID: generate_metrics(1.2, {"resource1": 1000, "resource2": 200, "resource3": 50}),
            NEW_LOG_FORWARDER_ID: generate_metrics(2.5, {"resource4": 4000}),
        }.get(config_id, [])

        await self.run_scaling_task(
            resource_cache_state={SUB_ID1: {EAST_US: {"resource1"}}},
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
        self.client.delete_log_forwarder.assert_awaited_once_with(NEW_LOG_FORWARDER_ID)
        self.assertEqual(
            self.cache,
            {
                SUB_ID1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID},
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                }
            },
        )

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    async def test_forwarders_are_coalesced(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.side_effect = lambda _c, config_id, _t: {
            OLD_LOG_FORWARDER_ID: generate_metrics(1.2, {"resource1": 1000, "resource2": 200, "resource3": 50}),
            NEW_LOG_FORWARDER_ID: generate_metrics(2.5, {"resource4": 4000}),
        }.get(config_id, [])

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
                            "resource4": OLD_LOG_FORWARDER_ID,
                        },
                        "configurations": {OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                    }
                }
            },
        )


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
