# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from logging import INFO, basicConfig
from unittest import TestCase
from unittest.mock import patch

# project
from tasks.task import Task, get_error_telemetry
from tasks.tests.common import TaskTestCase


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
        self.cred = self.patch_path("tasks.task.DefaultAzureCredential").return_value
        basicConfig(level=INFO)

    @patch.dict("tasks.task.environ", {"DD_TELEMETRY": "false", "DD_API_KEY": "123"}, clear=True)
    async def test_task_logging_disabled(self):
        task = DummyTask()
        self.assertFalse(task.telemetry_enabled)
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
        self.assertFalse(task.telemetry_enabled)
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

    # @patch.dict(
    #     "tasks.task.environ", {"DD_TELEMETRY": "true", "DD_API_KEY": "123", "CONTROL_PLANE_ID": "456"}, clear=True
    # )
    async def test_task_logging_enabled(self):
        self.env.update({"DD_TELEMETRY": "true", "DD_API_KEY": "123", "CONTROL_PLANE_ID": "456"})
        task = DummyTask()
        self.assertTrue(task.telemetry_enabled)
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
