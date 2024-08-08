# stdlib
from datetime import datetime, timedelta
from json import dumps
from typing import Any
from unittest.mock import AsyncMock, call, patch
from uuid import UUID

import pytest

# 3p
# project
from cache.assignment_cache import ASSIGNMENT_CACHE_BLOB, AssignmentCache, deserialize_assignment_cache
from cache.common import (
    FUNCTION_APP_PREFIX,
    STORAGE_ACCOUNT_PREFIX,
    STORAGE_ACCOUNT_TYPE,
    InvalidCacheError,
)
from cache.resources_cache import ResourceCache
from tasks.scaling_task import (
    FORWARDER_METRIC_CONTAINER_NAME,
    METRIC_COLLECTION_PERIOD_MINUTES,
    SCALING_TASK_NAME,
    ScalingTask,
)
from tasks.tests.common import TaskTestCase

sub_id1 = "decc348e-ca9e-4925-b351-ae56b0d9f811"
EAST_US = "eastus"
WEST_US = "westus"
log_forwarder_id = "d6fc2c757f9c"
log_forwarder_name = FUNCTION_APP_PREFIX + log_forwarder_id
storage_account_name = STORAGE_ACCOUNT_PREFIX + log_forwarder_id
rg1 = "test_lfo"


NEW_UUID = "04cb0e0b-f268-4349-aa32-93a5885365f5"
OLD_LOG_FORWARDER_ID = "5a095f74c60a"
NEW_LOG_FORWARDER_ID = "93a5885365f5"


class TestScalingTask(TaskTestCase):
    TASK_NAME = SCALING_TASK_NAME

    async def asyncSetUp(self) -> None:
        super().setUp()
        client = AsyncMock()
        self.patch_path("tasks.scaling_task.LogForwarderClient").return_value = client
        self.client = await client.__aenter__()
        self.client.create_log_forwarder.return_value = STORAGE_ACCOUNT_TYPE

        self.log = self.patch("log")
        self.uuid = self.patch("uuid4")
        self.uuid.return_value = UUID(NEW_UUID)

    @property
    def cache(self):
        success, cache = deserialize_assignment_cache(self.cache_value(ASSIGNMENT_CACHE_BLOB))
        self.assertTrue(success)
        return cache

    async def run_scaling_task(
        self, resource_cache_state: ResourceCache, assignment_cache_state: AssignmentCache, resource_group: str = rg1
    ):
        async with ScalingTask(
            dumps(resource_cache_state, default=list), dumps(assignment_cache_state), resource_group
        ) as task:
            await task.run()

    async def test_scaling_task_fails_without_valid_resource_cache(self):
        with self.assertRaises(InvalidCacheError):
            ScalingTask("invalid json", "{}", "lfo")

    async def test_reset_invalid_scaling_cache(self):
        invalid_cache: Any = "not valid"
        await self.run_scaling_task(
            {},
            invalid_cache,
            "lfo",
        )
        self.write_cache.assert_not_awaited()
        self.log.warning.assert_called_once_with("Assignment Cache is in an invalid format, task will reset the cache")

    async def test_new_regions_are_added(self):
        await self.run_scaling_task(
            resource_cache_state={sub_id1: {EAST_US: {"resource1", "resource2"}}},
            assignment_cache_state={},
        )

        self.client.create_log_forwarder.assert_called_once_with(EAST_US, NEW_LOG_FORWARDER_ID)
        expected_cache: AssignmentCache = {
            sub_id1: {
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
                sub_id1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                },
            },
        )

        self.client.delete_log_forwarder.assert_awaited_once_with(OLD_LOG_FORWARDER_ID)

        self.assertEqual(self.cache, {sub_id1: {}})

    async def test_regions_added_and_deleted(self):
        await self.run_scaling_task(
            resource_cache_state={sub_id1: {WEST_US: {"resource1", "resource2"}}},
            assignment_cache_state={
                sub_id1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                },
            },
        )

        self.client.create_log_forwarder.assert_called_once_with(WEST_US, NEW_LOG_FORWARDER_ID)
        self.client.delete_log_forwarder.assert_called_once_with(OLD_LOG_FORWARDER_ID)

        expected_cache: AssignmentCache = {
            sub_id1: {
                WEST_US: {
                    "resources": {"resource1": NEW_LOG_FORWARDER_ID, "resource2": NEW_LOG_FORWARDER_ID},
                    "configurations": {NEW_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE},
                }
            }
        }
        self.assertEqual(self.cache, expected_cache)

    async def test_log_forwarder_metrics_collected(self):
        current_time = (datetime.now()).timestamp()
        self.client.get_blob_metrics.return_value = [
            dumps(
                {
                    "timestamp": current_time,
                    "runtime": 211,
                    "resourceLogAmounts": {"5a095f74c60a": 4, "93a5885365f5": 6},
                }
            ),
            dumps(
                {
                    "timestamp": current_time,
                    "runtime": 199,
                    "resourceLogAmounts": {"5a095f74c60a": 4, "93a5885365f5": 6},
                }
            ),
        ]
        await self.run_scaling_task(
            resource_cache_state={sub_id1: {EAST_US: {"resource1", "resource2"}}},
            assignment_cache_state={
                sub_id1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                },
            },
        )

        self.client.get_blob_metrics.assert_called_once_with(OLD_LOG_FORWARDER_ID, FORWARDER_METRIC_CONTAINER_NAME)
        self.assertTrue(
            call("No valid metrics found for forwarder %s", OLD_LOG_FORWARDER_ID) not in self.log.info.call_args_list
        )

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    async def test_log_forwarders_scale_up_when_underscaled(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.return_value = {"function_execution_time": 29.045}

        await self.run_scaling_task(
            resource_cache_state={sub_id1: {EAST_US: {"resource1", "resource2"}}},
            assignment_cache_state={
                sub_id1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                },
            },
        )

        self.client.create_log_forwarder.assert_awaited_once_with(EAST_US, NEW_LOG_FORWARDER_ID)
        expected_cache: AssignmentCache = {
            sub_id1: {
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
        self.client.get_blob_metrics.return_value = [
            dumps(
                {
                    "timestamp": current_time,
                    "runtime": 211,
                    "resourceLogAmounts": {"5a095f74c60a": 4, "93a5885365f5": 6},
                }
            ),
            dumps(
                {"timestamp": old_time, "runtime": 199, "resourceLogAmounts": {"5a095f74c60a": 4, "93a5885365f5": 6}}
            ),
        ]

        await self.run_scaling_task(
            resource_cache_state={sub_id1: {EAST_US: {"resource1", "resource2"}}},
            assignment_cache_state={
                sub_id1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                },
            },
        )

        self.client.get_blob_metrics.assert_called_once_with(OLD_LOG_FORWARDER_ID, FORWARDER_METRIC_CONTAINER_NAME)
        self.assertTrue(
            call("No valid metrics found for forwarder %s", OLD_LOG_FORWARDER_ID) not in self.log.info.call_args_list
        )

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    @pytest.mark.skip("AZINTS-2684")
    async def test_log_forwarders_dont_scale_when_not_needed(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.return_value = {"function_execution_time": 22.78}

        await self.run_scaling_task(
            resource_cache_state={sub_id1: {EAST_US: {"resource1", "resource2"}}},
            assignment_cache_state={
                sub_id1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                },
            },
        )
        # TODO[AZINTS-2684]: Fix test as create_log_forwarder is awaited
        self.client.create_log_forwarder.assert_not_awaited()
        self.write_cache.assert_not_called()

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    @pytest.mark.skip("AZINTS-2684")
    async def test_new_resources_onboarded_during_scaling(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.return_value = {"function_execution_time": 2.5}
        await self.run_scaling_task(
            resource_cache_state={sub_id1: {EAST_US: {"resource1", "resource2", "resource3", "resource4"}}},
            assignment_cache_state={
                sub_id1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                },
            },
        )

        # TODO[AZINTS-2684]: Fix test as create_log_forwarder is awaited
        self.client.create_log_forwarder.assert_not_awaited()
        expected_cache: AssignmentCache = {
            sub_id1: {
                EAST_US: {
                    "resources": {
                        "resource1": OLD_LOG_FORWARDER_ID,
                        "resource2": OLD_LOG_FORWARDER_ID,
                        "resource3": OLD_LOG_FORWARDER_ID,
                        "resource4": OLD_LOG_FORWARDER_ID,
                    },
                    "configurations": {
                        OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                    },
                }
            },
        }
        # TODO[AZINTS-2684]: Fix test as self.cache is invalid
        self.assertEqual(self.cache, expected_cache)

    @patch.object(ScalingTask, "collect_forwarder_metrics", new_callable=AsyncMock)
    @pytest.mark.skip("AZINTS-2684")
    async def test_new_resources_onboard_to_the_least_busy_forwarder(self, collect_forwarder_metrics: AsyncMock):
        collect_forwarder_metrics.side_effect = lambda config_id, *_: {
            OLD_LOG_FORWARDER_ID: {"function_execution_time": 2.5},
            NEW_LOG_FORWARDER_ID: {"function_execution_time": 10.5},
        }[config_id]
        await self.run_scaling_task(
            resource_cache_state={sub_id1: {EAST_US: {"resource1", "resource2", "resource3", "resource4"}}},
            assignment_cache_state={
                sub_id1: {
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

        expected_cache: AssignmentCache = {
            sub_id1: {
                EAST_US: {
                    "resources": {
                        "resource1": OLD_LOG_FORWARDER_ID,
                        "resource2": NEW_LOG_FORWARDER_ID,
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
        # TODO[AZINTS-2684]: Fix test as self.cache is invalid
        self.assertEqual(self.cache, expected_cache)

    async def test_old_log_forwarder_metrics_not_collected(self):
        old_time = (datetime.now() - timedelta(minutes=(METRIC_COLLECTION_PERIOD_MINUTES + 1))).timestamp()
        self.client.get_blob_metrics.return_value = [
            dumps(
                {
                    "timestamp": old_time,
                    "runtime": 211,
                    "resourceLogAmounts": {"5a095f74c60a": 4, "93a5885365f5": 6},
                }
            ),
            dumps(
                {"timestamp": old_time, "runtime": 199, "resourceLogAmounts": {"5a095f74c60a": 4, "93a5885365f5": 6}}
            ),
        ]
        await self.run_scaling_task(
            resource_cache_state={sub_id1: {EAST_US: {"resource1", "resource2"}}},
            assignment_cache_state={
                sub_id1: {
                    EAST_US: {
                        "resources": {"resource1": OLD_LOG_FORWARDER_ID, "resource2": OLD_LOG_FORWARDER_ID},
                        "configurations": {
                            OLD_LOG_FORWARDER_ID: STORAGE_ACCOUNT_TYPE,
                        },
                    }
                },
            },
            resource_group="test_lfo",
        )

        self.client.get_blob_metrics.assert_called_once_with(OLD_LOG_FORWARDER_ID, FORWARDER_METRIC_CONTAINER_NAME)
        self.assertTrue(
            call("No valid metrics found for forwarder %s", OLD_LOG_FORWARDER_ID) in self.log.info.call_args_list
        )
