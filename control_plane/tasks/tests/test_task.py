# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from logging import INFO, basicConfig
from unittest import TestCase
from unittest.mock import ANY, call, patch

# project
import tasks.task as task_module
from tasks.client.datadog_api_client import StatusCode
from tasks.common import (
    CONTROL_PLANE_METRIC_PREFIX,
    TASK_RUN_COMPLETED_METRIC,
    TASK_STARTED_METRIC,
)
from tasks.task import Task, get_error_telemetry, task_main
from tasks.tests.common import AsyncMockClient, TaskTestCase


class TestGetErrorTelemetry(TestCase):
    def test_no_exception(self):
        self.assertEqual(get_error_telemetry(None), {})
        self.assertEqual(get_error_telemetry((None, None, None)), {})

    def test_with_exception(self):
        try:
            _ = 1 / 0
        except ZeroDivisionError as e:
            exc_info = (type(e), e, e.__traceback__)
            telemetry = get_error_telemetry(exc_info)
            self.assertEqual(telemetry["exception"], "ZeroDivisionError")
            self.assertIn("ZeroDivisionError: division by zero", telemetry["exc_info"])

    def test_with_custom_exception(self):
        class CustomError(Exception):
            pass

        try:
            raise CustomError("custom error occurred")
        except CustomError as e:
            exc_info = (type(e), e, e.__traceback__)
            telemetry = get_error_telemetry(exc_info)
            self.assertEqual(telemetry["exception"], "CustomError")
            self.assertIn("CustomError: custom error occurred", telemetry["exc_info"])


class DummyTask(Task):
    NAME = "dummy_task"

    async def run(self):
        self.log.error("Hello World")

    async def write_caches(self):
        pass


class TestTask(TaskTestCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        basicConfig(level=INFO)
        task_module.TELEMETRY_ENABLED = False

    @patch.dict("tasks.task.environ", {"DD_TELEMETRY": "false", "DD_API_KEY": "123"}, clear=True)
    async def test_task_logging_disabled(self):
        task = DummyTask()
        self.assertEqual(task._logs, [])
        self.assertEqual(
            task.tags, ["forwarder:lfocontrolplane", "task:dummy_task", "control_plane_id:unknown", "version:unknown"]
        )
        async with task:
            await task.run()
            self.assertEqual(task._logs, [])
        task._logs_client.submit_log.assert_not_awaited()  # type: ignore
        task._datadog_client.__aenter__.assert_called_once_with()  # type: ignore
        task._datadog_client.__aexit__.assert_called_once_with(None, None, None)  # type: ignore
        task._logs_client.submit_log.assert_not_called()  # type: ignore
        self.assertEqual(task._logs, [])

    @patch.dict("tasks.task.environ", {}, clear=True)
    async def test_task_logging_not_specified_is_disabled(self):
        task = DummyTask()
        self.assertEqual(task._logs, [])
        self.assertEqual(
            task.tags, ["forwarder:lfocontrolplane", "task:dummy_task", "control_plane_id:unknown", "version:unknown"]
        )
        async with task:
            await task.run()
            self.assertEqual(task._logs, [])
        task._logs_client.submit_log.assert_not_awaited()  # type: ignore
        task._datadog_client.__aenter__.assert_called_once_with()  # type: ignore
        task._datadog_client.__aexit__.assert_called_once_with(None, None, None)  # type: ignore
        task._logs_client.submit_log.assert_not_called()  # type: ignore
        self.assertEqual(task._logs, [])

    async def test_task_logging_enabled(self):
        self.env.update({"DD_TELEMETRY": "true", "DD_API_KEY": "123", "CONTROL_PLANE_ID": "456"})
        task_module.TELEMETRY_ENABLED = True
        task = DummyTask()
        self.assertEqual(task._logs, [])
        self.assertEqual(
            task.tags, ["forwarder:lfocontrolplane", "task:dummy_task", "control_plane_id:456", "version:unknown"]
        )
        async with task:
            await task.run()
            self.assertEqual(len(task._logs), 1)
            self.assertEqual(task._logs[0].message, "Hello World")
        task._logs_client.submit_log.assert_called_once()  # type: ignore
        task._datadog_client.__aenter__.assert_called_once_with()  # type: ignore
        task._datadog_client.__aexit__.assert_called_once_with(None, None, None)  # type: ignore
        self.assertEqual(task._logs, [])


class TestSubmitStatusUpdate(TaskTestCase):
    """Error statuses must reach the status endpoint on every run, not only the initial one.

    Suppressing them outside onboarding is why a scaling task stuck in a failed create/delete
    loop produced no status signal at all until telemetry was manually turned on.
    """

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        basicConfig(level=INFO)
        task_module.TELEMETRY_ENABLED = False
        self.status_client = AsyncMockClient()
        self.datadog_client = AsyncMockClient()
        self.datadog_client.submit_status_update = self.status_client
        self.patch_path("tasks.task.DatadogClient").return_value = self.datadog_client

    async def test_error_status_submitted_on_steady_state_run(self):
        task = DummyTask(is_initial_run=False)
        async with task:
            await task.submit_status_update("step", StatusCode.RESOURCE_CREATION_ERROR, "boom")
        self.status_client.assert_awaited_once_with(
            "dummy_task.step", StatusCode.RESOURCE_CREATION_ERROR, "boom", task.execution_id, "unknown", "unknown"
        )

    async def test_ok_status_suppressed_on_steady_state_run(self):
        task = DummyTask(is_initial_run=False)
        async with task:
            await task.submit_status_update("step", StatusCode.OK, "fine")
        self.status_client.assert_not_awaited()

    async def test_ok_status_submitted_on_initial_run(self):
        task = DummyTask(is_initial_run=True)
        async with task:
            await task.submit_status_update("step", StatusCode.OK, "fine")
        self.status_client.assert_awaited_once()


class TestStatusUpdateNeverAborts(TaskTestCase):
    """A failing status endpoint must not abort its caller.

    These calls are awaited from inside except handlers that still have cleanup left to run, so
    an unreachable status endpoint must never become the reason a storage account is leaked.
    """

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        basicConfig(level=INFO)
        task_module.TELEMETRY_ENABLED = False
        self.status_client = AsyncMockClient()
        self.datadog_client = AsyncMockClient()
        self.datadog_client.submit_status_update = self.status_client
        self.patch_path("tasks.task.DatadogClient").return_value = self.datadog_client

    async def test_status_failure_is_logged_where_telemetry_can_collect_it(self):
        # the ListHandler feeding Datadog log submission is attached to self.log (a child logger),
        # and records propagate up but never down - logging this on the module logger would make a
        # status endpoint failing on every run completely silent in the telemetry this PR adds
        task_module.TELEMETRY_ENABLED = True
        self.addCleanup(setattr, task_module, "TELEMETRY_ENABLED", False)
        self.status_client.side_effect = ConnectionError("status endpoint down")
        task = DummyTask(is_initial_run=False)
        async with task:
            await task.submit_status_update("step", StatusCode.RESOURCE_CREATION_ERROR, "boom")
            messages = [record.getMessage() for record in task._logs]
        self.assertTrue(
            any("Failed to submit status update" in message for message in messages),
            f"status failure missing from telemetry logs: {messages}",
        )

    async def test_status_endpoint_failure_does_not_propagate(self):
        self.status_client.side_effect = ConnectionError("status endpoint down")
        task = DummyTask(is_initial_run=False)
        async with task:
            await task.submit_status_update("step", StatusCode.RESOURCE_CREATION_ERROR, "boom")
        self.status_client.assert_awaited_once()


class TestTaskRunMetrics(TaskTestCase):
    """task_started paired with task_run_completed makes a run killed mid-flight visible.

    A function host terminated at its timeout never reaches teardown, so it logs nothing at all;
    the started-minus-completed delta is the only trace it leaves.
    """

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        basicConfig(level=INFO)
        task_module.TELEMETRY_ENABLED = True
        self.patch_path("tasks.task.read_cache")

    async def test_started_and_completed_emitted_for_a_clean_run(self):
        await task_main(DummyTask, [])
        self.statsd.count.assert_has_calls(
            [
                call(CONTROL_PLANE_METRIC_PREFIX + TASK_STARTED_METRIC, 1, tags=ANY),
                call(CONTROL_PLANE_METRIC_PREFIX + TASK_RUN_COMPLETED_METRIC, 1, tags=ANY),
            ],
            any_order=True,
        )

    async def test_completed_not_emitted_when_the_run_raises(self):
        class FailingTask(DummyTask):
            async def run(self):
                raise ValueError("boom")

        with self.assertRaises(ValueError):
            await task_main(FailingTask, [])

        emitted = [c.args[0] for c in self.statsd.count.call_args_list]
        self.assertIn(CONTROL_PLANE_METRIC_PREFIX + TASK_STARTED_METRIC, emitted)
        self.assertNotIn(CONTROL_PLANE_METRIC_PREFIX + TASK_RUN_COMPLETED_METRIC, emitted)

    async def test_no_metrics_emitted_when_telemetry_is_disabled(self):
        task_module.TELEMETRY_ENABLED = False
        await task_main(DummyTask, [])
        self.statsd.count.assert_not_called()
