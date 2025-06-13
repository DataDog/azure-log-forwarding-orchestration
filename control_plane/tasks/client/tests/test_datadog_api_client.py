# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from typing import Any
from unittest.mock import Mock

# project
from tasks.client.datadog_api_client import DatadogClient, StatusCode, create_submit_status_payload
from tasks.tests.common import AsyncMockClient, AsyncTestCase


class TestDatadogClient(AsyncTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.dd_site = "datadoghq.com"
        self.api_key = "test-api-key"
        self.session = AsyncMockClient()
        self.patch_path("tasks.client.datadog_api_client.ClientSession", return_value=self.session)
        self.post_mock = AsyncMockClient()
        self.session.post = Mock(return_value=self.post_mock)

    def test_create_submit_status_payload(self):
        """Test that create_submit_status_payload creates the correct payload structure."""
        payload = create_submit_status_payload(
            step="test_step",
            status=StatusCode.OK,
            message="test message",
            execution_id="test-execution-123",
            version="1.0.0",
            control_plane_id="test-cp-123",
        )

        self.assertEqual(payload["id"], "test-execution-123")
        self.assertEqual(payload["type"], "workflow_id")
        self.assertEqual(payload["attributes"]["workflow_type"], "add_azure_log_forwarder")
        self.assertEqual(payload["attributes"]["step_id"], "test_step")
        self.assertEqual(payload["attributes"]["status"], "OK")
        self.assertEqual(payload["attributes"]["message"], "test message")
        self.assertEqual(payload["attributes"]["metadata"]["version"], "1.0.0")
        self.assertEqual(payload["attributes"]["metadata"]["control_plane_id"], "test-cp-123")

        # Test with None control_plane_id
        payload = create_submit_status_payload(
            step="test_step",
            status=StatusCode.OK,
            message="test message",
            execution_id="test-execution-123",
            version="1.0.0",
            control_plane_id=None,
        )
        self.assertIsNone(payload["attributes"]["metadata"]["control_plane_id"])

    async def test_client_initialization(self):
        """Test DatadogClient initialization and context manager."""
        async with DatadogClient(dd_site=self.dd_site, api_key=self.api_key) as c:
            client = c

        self.assertEqual(client.dd_site, self.dd_site)
        self.assertEqual(client.api_key, self.api_key)
        self.assertIsNotNone(client.session)
        self.session.__aenter__.assert_called_once()
        self.session.__aexit__.assert_called_once()

    async def test_client_url_generation(self):
        """Test URL generation."""
        async with DatadogClient(dd_site=self.dd_site, api_key=self.api_key) as c:
            client = c

        url = client._get_url("test/endpoint")
        self.assertEqual(url, f"https://api.{self.dd_site}/test/endpoint")

    async def test_client_headers(self):
        """Test header generation."""
        async with DatadogClient(dd_site=self.dd_site, api_key=self.api_key) as c:
            client = c

        headers = client._get_headers()
        self.assertEqual(headers, {"dd-api-key": self.api_key, "Content-Type": "application/json"})

    async def test_submit_status_update_success(self):
        """Test successful status update submission."""
        mock_response = Mock()
        mock_response.status = 200
        self.session.post.return_value.__aenter__.return_value = mock_response

        async with DatadogClient(dd_site=self.dd_site, api_key=self.api_key) as c:
            client = c
            status = await client.submit_status_update(
                step="test_step",
                status=StatusCode.OK,
                message="test message",
                execution_id="test-execution-123",
                version="1.0.0",
                control_plane_id="test-cp-123",
            )

        self.assertEqual(status, 200)
        self.session.post.assert_called_once()
        call_args = self.session.post.call_args
        self.assertEqual(
            call_args[0][0], f"https://api.{self.dd_site}/api/unstable/integration/azure/logforwarding/status"
        )
        self.assertEqual(call_args[1]["headers"], client._get_headers())
        self.assertIn("data", call_args[1]["json"])

    async def test_submit_status_update_error(self):
        """Test error handling during status update."""

        async def mock_post(*args: Any, **kwargs: Any) -> Mock:
            raise Exception("Network error")

        self.post_mock.__aexit__.side_effect = mock_post

        async with DatadogClient(dd_site=self.dd_site, api_key=self.api_key) as client:
            with self.assertRaises(Exception) as context:
                await client.submit_status_update(
                    step="test_step",
                    status=StatusCode.UNKNOWN_ERROR,
                    message="error message",
                    execution_id="test-execution-123",
                    version="1.0.0",
                    control_plane_id=None,
                )
            self.assertEqual(str(context.exception), "Network error")

    async def test_context_manager_cleanup(self):
        """Test proper cleanup in context manager."""
        async with DatadogClient(dd_site=self.dd_site, api_key=self.api_key) as client:
            self.assertIsNotNone(client.session)

        self.session.__aexit__.assert_called_once()

    async def test_submit_status_update_invalid_response(self):
        """Test handling of invalid response status."""
        mock_response = Mock()
        mock_response.status = 500
        self.session.post.return_value.__aenter__.return_value = mock_response

        async with DatadogClient(dd_site=self.dd_site, api_key=self.api_key) as c:
            client = c

            status = await client.submit_status_update(
                step="test_step",
                status=StatusCode.OK,
                message="test message",
                execution_id="test-execution-123",
                version="1.0.0",
                control_plane_id="test-cp-123",
            )

        self.assertEqual(status, 500)
