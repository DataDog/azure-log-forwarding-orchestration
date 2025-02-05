# stdlib
from unittest import TestCase

# project
from tasks.task import get_error_telemetry


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
