# stdlib
from asyncio import sleep
from json import dumps
from os import environ
from typing import cast
from unittest.mock import ANY, DEFAULT, AsyncMock, MagicMock, Mock, patch

# 3p
from aiosonic.exceptions import RequestTimeout
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError, ServiceResponseTimeoutError
from datadog_api_client.v2.model.metric_intake_type import MetricIntakeType
from datadog_api_client.v2.model.metric_payload import MetricPayload
from datadog_api_client.v2.model.metric_point import MetricPoint
from datadog_api_client.v2.model.metric_resource import MetricResource
from datadog_api_client.v2.model.metric_series import MetricSeries
from tenacity import RetryError

# project
from cache.metric_blob_cache import MetricBlobEntry
from tasks.client.log_forwarder_client import (
    MAX_ATTEMPS,
    LogForwarderClient,
)
from tasks.common import (
    FORWARDER_CONTAINER_APP_PREFIX,
    FORWARDER_MANAGED_ENVIRONMENT_PREFIX,
    FORWARDER_STORAGE_ACCOUNT_PREFIX,
    get_container_app_name,
    get_managed_env_name,
    get_storage_account_name,
)
from tasks.tests.common import (
    AsyncMockClient,
    AsyncTestCase,
    AzureModelMatcher,
    async_generator,
    mock,
)
from tasks.tests.test_scaling_task import generate_metrics, minutes_ago

SUB_ID1 = "decc348e-ca9e-4925-b351-ae56b0d9f811"
EAST_US = "eastus"
WEST_US = "westus"
NEW_ZEALAND_NORTH = "newzealandnorth"
CONFIG_ID1 = "d6fc2c757f9c"
CONFIG_ID2 = "e8d5222d1c46"
CONFIG_ID3 = "619fff16cae1"
CONTROL_PLANE_ID = "e90ecb54476d"
MANAGED_ENV_EAST_US_NAME = f"{FORWARDER_MANAGED_ENVIRONMENT_PREFIX}{CONTROL_PLANE_ID}-{EAST_US}"
CONTAINER_APP_NAME = f"{FORWARDER_CONTAINER_APP_PREFIX}{CONFIG_ID1}"
STORAGE_ACCOUNT_NAME = f"{FORWARDER_STORAGE_ACCOUNT_PREFIX}{CONFIG_ID1}"
RESOURCE_GROUP_NAME = "test_lfo"


LOG_FORWARDER_CLIENT_SETTINGS: dict[str, str] = {
    "AzureWebJobsStorage": "connection-string",
    "DD_API_KEY": "123123",
    "DD_APP_KEY": "456456",
    "DD_SITE": "datadoghq.com",
    "FORWARDER_IMAGE": "ddlfo.azurecr.io/blobforwarder:latest",
    "CONTROL_PLANE_REGION": EAST_US,
    "CONTROL_PLANE_ID": "e90ecb54476d",
    "SHOULD_SUBMIT_METRICS": "True",
}


class FakeHttpError(HttpResponseError):
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        self.message = str({"code": f"something related to {self.status_code}"})

    reason = None
    error = None

    def __eq__(self, value: object) -> bool:
        return isinstance(value, FakeHttpError) and value.status_code == self.status_code


class MockedLogForwarderClient(LogForwarderClient):
    """Used for typing since we know the underlying clients will be mocks"""

    container_apps_client: AsyncMock
    storage_client: AsyncMock
    _datadog_client: AsyncMock
    metrics_client: AsyncMock


FAKE_METRIC_BLOBS: list[MetricBlobEntry] = [
    {
        "timestamp": 1723040910,
        "runtime_seconds": 2.80,
        "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 6},
        "resource_log_bytes": {"5a095f74c60a": 400, "93a5885365f5": 600},
    },
    {
        "timestamp": 1723040911,
        "runtime_seconds": 2.81,
        "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 7},
        "resource_log_bytes": {"5a095f74c60a": 400, "93a5885365f5": 701},
    },
]

FAKE_METRIC_PAYLOAD = MetricPayload(
    series=[
        MetricSeries(
            metric="azure.lfo.forwarder.resource_log_volume",
            type=MetricIntakeType.UNSPECIFIED,
            points=[MetricPoint(timestamp=1723040910, value=10), MetricPoint(timestamp=1723040911, value=11)],
            resources=[MetricResource(name="dd-log-forwarder-test", type="logforwarder")],
        ),
        MetricSeries(
            metric="azure.lfo.forwarder.resource_log_bytes",
            type=MetricIntakeType.UNSPECIFIED,
            points=[MetricPoint(timestamp=1723040910, value=1000), MetricPoint(timestamp=1723040911, value=1101)],
            resources=[MetricResource(name="dd-log-forwarder-test", type="logforwarder")],
        ),
        MetricSeries(
            metric="azure.lfo.forwarder.runtime_seconds",
            type=MetricIntakeType.UNSPECIFIED,
            points=[MetricPoint(timestamp=1723040910, value=2.80), MetricPoint(timestamp=1723040911, value=2.81)],
            resources=[MetricResource(name="dd-log-forwarder-test", type="logforwarder")],
        ),
    ],
)


class TestLogForwarderClient(AsyncTestCase):
    async def asyncSetUp(self) -> None:
        p = patch.dict(environ, LOG_FORWARDER_CLIENT_SETTINGS, clear=True)
        p.start()
        self.addCleanup(p.stop)
        self.log = mock()
        self.client: MockedLogForwarderClient = cast(
            MockedLogForwarderClient,
            LogForwarderClient(
                log=self.log, credential=AsyncMock(), subscription_id=SUB_ID1, resource_group=RESOURCE_GROUP_NAME
            ),
        )
        await self.client.__aexit__(None, None, None)
        self.client.container_apps_client = AsyncMockClient()
        self.client.storage_client = AsyncMockClient()
        self.client._datadog_client = AsyncMockClient()
        self.client.metrics_client = AsyncMock()
        self.client.storage_client.storage_accounts.list_keys = AsyncMock(return_value=Mock(keys=[Mock(value="key")]))
        self.container_client_class = self.patch_path("tasks.client.log_forwarder_client.ContainerClient")

        self.container_client = AsyncMockClient()
        self.container_client_class.from_connection_string.return_value = self.container_client
        self.container_client.get_blob_client = MagicMock()
        self.blob_client = AsyncMockClient()
        self.container_client.get_blob_client.return_value = self.blob_client

    async def test_create_log_forwarder_creates_resources(self):
        # set up blob forwarder data
        (await self.container_client.download_blob()).content_as_bytes.return_value = b"some data"

        async with self.client:
            await self.client.create_log_forwarder(EAST_US, CONFIG_ID1)

        # storage account
        storage_create: AsyncMock = self.client.storage_client.storage_accounts.begin_create
        storage_create.assert_awaited_once_with(
            RESOURCE_GROUP_NAME,
            STORAGE_ACCOUNT_NAME,
            AzureModelMatcher(
                {
                    "sku": {"name": "Standard_LRS"},
                    "kind": "StorageV2",
                    "location": EAST_US,
                    "public_network_access": "Enabled",
                }
            ),
        )
        (await storage_create()).result.assert_awaited_once_with()
        # container job
        container_app_job_create: AsyncMock = self.client.container_apps_client.jobs.begin_create_or_update
        container_app_job_create.assert_awaited_once_with(
            RESOURCE_GROUP_NAME,
            CONTAINER_APP_NAME,
            AzureModelMatcher(
                {
                    "location": EAST_US,
                    "environment_id": "/subscriptions/decc348e-ca9e-4925-b351-ae56b0d9f811/resourcegroups/test_lfo/providers/microsoft.app/managedenvironments/dd-log-forwarder-env-e90ecb54476d-eastus",
                    "configuration": {
                        "secrets": [
                            {"name": "dd-api-key", "value": "123123"},
                            {
                                "name": "connection-string",
                                "value": "DefaultEndpointsProtocol=https;AccountName=ddlogstoraged6fc2c757f9c;AccountKey=key;EndpointSuffix=core.windows.net",
                            },
                        ],
                        "trigger_type": "Schedule",
                        "replica_timeout": 1800,
                        "replica_retry_limit": 1,
                        "schedule_trigger_config": {
                            "cron_expression": "* * * * *",
                            "parallelism": 1,
                            "replica_completion_count": 1,
                        },
                    },
                    "template": {
                        "containers": [
                            {
                                "image": "ddlfo.azurecr.io/blobforwarder:latest",
                                "name": "forwarder",
                                "env": [
                                    {"name": "AzureWebJobsStorage", "secret_ref": "connection-string"},
                                    {"name": "DD_API_KEY", "secret_ref": "dd-api-key"},
                                    {"name": "DD_SITE", "value": "datadoghq.com"},
                                    {"name": "CONTROL_PLANE_ID", "value": "e90ecb54476d"},
                                    {"name": "CONFIG_ID", "value": "d6fc2c757f9c"},
                                ],
                                "resources": {"cpu": 2.0, "memory": "4Gi"},
                            }
                        ]
                    },
                }
            ),
        )
        (await container_app_job_create()).result.assert_awaited_once_with()

    async def test_create_log_forwarder_in_unsupported_region_falls_back_to_control_plane_region(self):
        (await self.container_client.download_blob()).content_as_bytes.return_value = b"some data"

        async with self.client:
            await self.client.create_log_forwarder(NEW_ZEALAND_NORTH, config_id=CONFIG_ID1)

        # storage account
        storage_create: AsyncMock = self.client.storage_client.storage_accounts.begin_create
        storage_create.assert_awaited_once_with(
            RESOURCE_GROUP_NAME,
            STORAGE_ACCOUNT_NAME,
            AzureModelMatcher(
                {
                    "sku": {"name": "Standard_LRS"},
                    "kind": "StorageV2",
                    "location": NEW_ZEALAND_NORTH,
                    "public_network_access": "Enabled",
                }
            ),
        )
        (await storage_create()).result.assert_awaited_once_with()
        # container job
        container_app_job_create: AsyncMock = self.client.container_apps_client.jobs.begin_create_or_update
        container_app_job_create.assert_awaited_once_with(
            RESOURCE_GROUP_NAME,
            CONTAINER_APP_NAME,
            AzureModelMatcher(
                {
                    "location": EAST_US,
                    "environment_id": "/subscriptions/decc348e-ca9e-4925-b351-ae56b0d9f811/resourcegroups/test_lfo/providers/microsoft.app/managedenvironments/dd-log-forwarder-env-e90ecb54476d-eastus",
                    "configuration": {
                        "secrets": [
                            {"name": "dd-api-key", "value": "123123"},
                            {
                                "name": "connection-string",
                                "value": "DefaultEndpointsProtocol=https;AccountName=ddlogstoraged6fc2c757f9c;AccountKey=key;EndpointSuffix=core.windows.net",
                            },
                        ],
                        "trigger_type": "Schedule",
                        "replica_timeout": 1800,
                        "replica_retry_limit": 1,
                        "schedule_trigger_config": {
                            "cron_expression": "* * * * *",
                            "parallelism": 1,
                            "replica_completion_count": 1,
                        },
                    },
                    "template": {
                        "containers": [
                            {
                                "image": "ddlfo.azurecr.io/blobforwarder:latest",
                                "name": "forwarder",
                                "env": [
                                    {"name": "AzureWebJobsStorage", "secret_ref": "connection-string"},
                                    {"name": "DD_API_KEY", "secret_ref": "dd-api-key"},
                                    {"name": "DD_SITE", "value": "datadoghq.com"},
                                    {"name": "CONTROL_PLANE_ID", "value": "e90ecb54476d"},
                                    {"name": "CONFIG_ID", "value": "d6fc2c757f9c"},
                                ],
                                "resources": {"cpu": 2.0, "memory": "4Gi"},
                            }
                        ]
                    },
                }
            ),
        )
        (await container_app_job_create()).result.assert_awaited_once_with()

    async def test_background_tasks_awaited(self):
        m = Mock()

        async def background_task():
            await sleep(0.05)
            m()

        async with LogForwarderClient(self.log, Mock(), "sub1", "rg1") as client:
            for _ in range(3):
                client.submit_background_task(background_task())
            failing_task_error = Exception("test")
            client.submit_background_task(AsyncMock(side_effect=failing_task_error)())

        self.assertEqual(m.call_count, 3)
        self.log.error.assert_called_once_with(
            "Background task failed with an exception",
            exc_info=failing_task_error,
            extra={"subscription_id": "sub1", "resource_group": "rg1"},
        )

    async def test_create_log_forwarder_no_keys(self):
        self.client.storage_client.storage_accounts.list_keys = AsyncMock(return_value=Mock(keys=[]))
        with self.assertRaises(ValueError) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US, CONFIG_ID1)
        self.assertIn("No keys found for storage account", str(ctx.exception))

    async def test_create_log_forwarder_managed_env_failure(self):
        (
            await self.client.container_apps_client.managed_environments.begin_create_or_update()
        ).result.side_effect = Exception("400: ASP creation failed")
        with self.assertRaises(Exception) as ctx:
            async with self.client:
                await self.client.create_log_forwarder_managed_environment(EAST_US, wait=True)
        self.assertIn("400: ASP creation failed", str(ctx.exception))

    async def test_create_log_forwarder_storage_account_failure(self):
        (await self.client.storage_client.storage_accounts.begin_create()).result.side_effect = Exception(
            "400: Storage Account creation failed"
        )
        with self.assertRaises(Exception) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US, CONFIG_ID1)
        self.assertIn("400: Storage Account creation failed", str(ctx.exception))

    async def test_create_log_forwarder_container_app_failure(self):
        (await self.client.container_apps_client.jobs.begin_create_or_update()).result.side_effect = Exception(
            "400: Function App creation failed"
        )
        with self.assertRaises(Exception) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US, CONFIG_ID1)
        self.assertIn("400: Function App creation failed", str(ctx.exception))

    async def test_delete_log_forwarder(self):
        async with self.client as client:
            success = await client.delete_log_forwarder(CONFIG_ID1)
        self.assertTrue(success)
        self.client.container_apps_client.jobs.begin_delete.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, CONTAINER_APP_NAME
        )
        self.client.storage_client.storage_accounts.delete.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME
        )

    async def test_delete_log_forwarder_ignore_resource_not_found(self):
        self.client.container_apps_client.jobs.begin_delete.side_effect = ResourceNotFoundError()
        self.client.storage_client.storage_accounts.delete.side_effect = ResourceNotFoundError()
        async with self.client as client:
            success = await client.delete_log_forwarder(CONFIG_ID1)
        self.assertTrue(success)
        self.client.container_apps_client.jobs.begin_delete.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, CONTAINER_APP_NAME
        )
        self.client.storage_client.storage_accounts.delete.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME
        )

    async def test_delete_log_forwarder_not_raise_error(self):
        self.client.container_apps_client.jobs.begin_delete.side_effect = FakeHttpError(400)
        async with self.client as client:
            success = await client.delete_log_forwarder(CONFIG_ID1, raise_error=False)
        self.assertFalse(success)
        self.client.container_apps_client.jobs.begin_delete.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, CONTAINER_APP_NAME
        )
        self.client.storage_client.storage_accounts.delete.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME
        )

    async def test_delete_log_forwarder_makes_3_retryable_attempts_default(self):
        self.client.container_apps_client.jobs.begin_delete.side_effect = FakeHttpError(429)
        with self.assertRaises(RetryError) as ctx:
            async with self.client as client:
                await client.delete_log_forwarder(CONFIG_ID1)
        self.assertEqual(ctx.exception.last_attempt.exception(), FakeHttpError(429))
        self.assertCalledTimesWith(
            self.client.container_apps_client.jobs.begin_delete, 3, RESOURCE_GROUP_NAME, CONTAINER_APP_NAME
        )
        self.assertCalledTimesWith(
            self.client.storage_client.storage_accounts.delete, 3, RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME
        )

    async def test_delete_log_forwarder_makes_5_retryable_attempts(self):
        self.client.container_apps_client.jobs.begin_delete.side_effect = FakeHttpError(529)
        self.client.storage_client.storage_accounts.delete.side_effect = ResourceNotFoundError()
        with self.assertRaises(RetryError) as ctx:
            async with self.client as client:
                await client.delete_log_forwarder(CONFIG_ID1, max_attempts=5)
        self.assertEqual(ctx.exception.last_attempt.exception(), FakeHttpError(529))
        self.assertCalledTimesWith(
            self.client.container_apps_client.jobs.begin_delete, 5, RESOURCE_GROUP_NAME, CONTAINER_APP_NAME
        )
        self.assertCalledTimesWith(
            self.client.storage_client.storage_accounts.delete, 5, RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME
        )

    async def test_delete_log_forwarder_env_calls_delete(self):
        # GIVEN
        env_name = get_managed_env_name(EAST_US, CONTROL_PLANE_ID)

        # WHEN
        async with self.client as client:
            success = await client.delete_log_forwarder_env(EAST_US)

        # THEN
        self.assertTrue(success)
        self.client.container_apps_client.managed_environments.begin_delete.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, env_name
        )

    async def test_delete_log_forwarder_env_ignore_resource_not_found(self):
        # GIVEN
        env_name = get_managed_env_name(EAST_US, CONTROL_PLANE_ID)
        self.client.container_apps_client.managed_environments.begin_delete.side_effect = ResourceNotFoundError()

        # WHEN
        async with self.client as client:
            success = await client.delete_log_forwarder_env(EAST_US)

        # THEN
        self.assertTrue(success)
        self.client.container_apps_client.managed_environments.begin_delete.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, env_name
        )

    async def test_delete_log_forwarder_env_does_not_raise_error_when_raise_error_false(self):
        # GIVEN
        env_name = get_managed_env_name(EAST_US, CONTROL_PLANE_ID)
        self.client.container_apps_client.managed_environments.begin_delete.side_effect = FakeHttpError(400)

        # WHEN
        async with self.client as client:
            success = await client.delete_log_forwarder_env(EAST_US, raise_error=False)

        # THEN
        self.assertFalse(success)
        self.client.container_apps_client.managed_environments.begin_delete.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, env_name
        )

    async def test_delete_log_forwarder_env_makes_5_retryable_attempts(self):
        # GIVEN
        env_name = get_managed_env_name(EAST_US, CONTROL_PLANE_ID)
        self.client.container_apps_client.managed_environments.begin_delete.side_effect = FakeHttpError(529)

        # WHEN
        with self.assertRaises(RetryError) as ctx:
            async with self.client as client:
                await client.delete_log_forwarder_env(EAST_US, max_attempts=5)

        # THEN
        self.assertEqual(ctx.exception.last_attempt.exception(), FakeHttpError(529))
        self.assertCalledTimesWith(
            self.client.container_apps_client.managed_environments.begin_delete, 5, RESOURCE_GROUP_NAME, env_name
        )

    async def test_get_log_forwarder_managed_environment_returns_id_on_success(self):
        # GIVEN
        env_id = "fake_id"

        mocked_env = Mock(id=env_id)

        self.client.container_apps_client.managed_environments.get.return_value = mocked_env

        # WHEN
        async with self.client as client:
            result = await client.get_log_forwarder_managed_environment(WEST_US)

        # THEN
        self.assertEqual(result, env_id)

    async def test_get_log_forwarder_managed_environment_returns_none_on_failure(self):
        # GIVEN
        self.client.container_apps_client.managed_environments.get.side_effect = ResourceNotFoundError()

        # WHEN
        async with self.client as client:
            result = await client.get_log_forwarder_managed_environment(WEST_US)

        # THEN
        self.assertIsNone(result)

    async def test_get_log_forwarder_managed_env_unsupported_region_defaults_to_control_plane_region(self):
        self.client.container_apps_client.managed_environments.get.return_value = mock(id="fake_id")

        async with self.client:
            res = await self.client.get_log_forwarder_managed_environment(NEW_ZEALAND_NORTH)

        self.assertEqual("fake_id", res)
        self.client.container_apps_client.managed_environments.get.assert_called_once_with(
            RESOURCE_GROUP_NAME, f"dd-log-forwarder-env-{CONTROL_PLANE_ID}-{EAST_US}"
        )

    async def test_delete_log_forwarder_doesnt_retry_after_second_unretryable(self):
        self.client.container_apps_client.jobs.begin_delete.side_effect = [FakeHttpError(429), FakeHttpError(400)]
        self.client.storage_client.storage_accounts.delete.side_effect = ResourceNotFoundError()

        with self.assertRaises(FakeHttpError) as ctx:
            async with self.client as client:
                await client.delete_log_forwarder(CONFIG_ID1)
        self.assertEqual(ctx.exception, FakeHttpError(400))
        self.assertCalledTimesWith(
            self.client.container_apps_client.jobs.begin_delete, 2, RESOURCE_GROUP_NAME, CONTAINER_APP_NAME
        )
        self.assertCalledTimesWith(
            self.client.storage_client.storage_accounts.delete, 2, RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME
        )

    async def test_get_blob_metrics_standard_execution(self):
        (await self.blob_client.download_blob()).readall.return_value = b"hi\nbye"

        async with self.client as client:
            res = await client.get_blob_metrics_lines("test", EAST_US)
            self.assertEqual(res, ["hi", "bye", "hi", "bye"])

    async def test_get_blob_metrics_missing_blob(self):
        (await self.blob_client.download_blob()).readall.return_value = b"hi\nbye"
        self.blob_client.download_blob.side_effect = [ResourceNotFoundError(), DEFAULT]

        async with self.client as client:
            res = await client.get_blob_metrics_lines("test", EAST_US)
            self.assertEqual(res, ["hi", "bye"])

    async def test_get_blob_timeout_retries(self):
        (await self.blob_client.download_blob()).readall.return_value = b"hi\nbye"
        # These side effects will allow the first call to download blob to succeed
        # The second call will fail with a timeout error
        # The method will retry three times until it succeeds
        self.blob_client.download_blob.side_effect = [
            DEFAULT,
            ServiceResponseTimeoutError("oops"),
            ServiceResponseTimeoutError("oops"),
            ServiceResponseTimeoutError("oops"),
            DEFAULT,
        ]

        async with self.client as client:
            res = await client.get_blob_metrics_lines("test", EAST_US)
            self.assertEqual(res, ["hi", "bye", "hi", "bye"])
            self.assertEqual(self.blob_client.download_blob.call_count, 6)  # 1 call is from where res_str is set

    async def test_get_blob_max_retries(self):
        self.blob_client.download_blob.side_effect = ServiceResponseTimeoutError("oops")

        async with self.client as client:
            await client.get_blob_metrics_lines("test", EAST_US)
        self.assertEqual(self.blob_client.download_blob.call_count, 2 * MAX_ATTEMPS)
        self.log.error.assert_called_with(
            "Unable to fetch metrics in %s for forwarder %s:\n%s",
            ANY,
            "test",
            "Max retries attempted, failed due to:\noops",
            extra={"subscription_id": "decc348e-ca9e-4925-b351-ae56b0d9f811", "resource_group": "test_lfo"},
        )

    async def test_get_blob_unretryable_exception(self):
        self.blob_client.download_blob.side_effect = FakeHttpError(402)

        async with self.client as client:
            await client.get_blob_metrics_lines("test", EAST_US)
        self.assertEqual(self.blob_client.download_blob.call_count, 2)
        self.log.error.assert_called_with(
            "Unable to fetch metrics in %s for forwarder %s:\n%s",
            ANY,
            "test",
            "HttpResponseError with Response Code: 402\nError: {'code': 'something related to 402'}",
            extra={"subscription_id": "decc348e-ca9e-4925-b351-ae56b0d9f811", "resource_group": "test_lfo"},
        )

    async def test_submit_metrics_normal_execution(self):
        self.client.should_submit_metrics = True
        self.client.metrics_client.submit_metrics.return_value = {}
        async with self.client as client:
            await client.submit_log_forwarder_metrics("test", FAKE_METRIC_BLOBS)

        self.client.metrics_client.submit_metrics.assert_called_once_with(body=FAKE_METRIC_PAYLOAD)

    async def test_submit_metrics_retries(self):
        self.client.should_submit_metrics = True
        self.client.metrics_client.submit_metrics.side_effect = [RequestTimeout(), RequestTimeout(), DEFAULT]
        self.client.metrics_client.submit_metrics.return_value = {}
        self.client.metrics_client.submit_metrics.side_effect = RequestTimeout()
        with self.assertRaises(RetryError) as ctx:
            async with self.client as client:
                await client.submit_log_forwarder_metrics("test", FAKE_METRIC_BLOBS)
        self.client.metrics_client.submit_metrics.assert_called_with(body=FAKE_METRIC_PAYLOAD)
        self.assertEqual(self.client.metrics_client.submit_metrics.call_count, MAX_ATTEMPS)

        self.assertIsInstance(ctx.exception.last_attempt.exception(), RequestTimeout)

    async def test_submit_metrics_nonretryable_exception(self):
        self.client.should_submit_metrics = True
        self.client.metrics_client.submit_metrics.side_effect = FakeHttpError(404)
        with self.assertRaises(FakeHttpError):
            async with self.client as client:
                await client.submit_log_forwarder_metrics("test", FAKE_METRIC_BLOBS)
        self.client.metrics_client.submit_metrics.assert_called_with(body=FAKE_METRIC_PAYLOAD)
        self.assertEqual(self.client.metrics_client.submit_metrics.call_count, 1)

    async def test_old_log_forwarder_metrics_are_ignored(self):
        metrics = generate_metrics(100, {"resource1": 4, "resource2": 6}, offset_mins=16)
        self.client.get_blob_metrics_lines = AsyncMock(return_value=list(map(dumps, metrics)))

        async with self.client as client:
            res = await client.collect_forwarder_metrics(CONFIG_ID1, EAST_US, oldest_valid_timestamp=minutes_ago(15))

        self.client.get_blob_metrics_lines.assert_called_once_with(CONFIG_ID1, EAST_US)
        self.assertEqual(res, [])

    async def test_submit_metrics_errors_logged(self):
        self.client.should_submit_metrics = True
        self.client.metrics_client.submit_metrics.return_value = {
            "errors": [
                "oops something went wrong",
            ]
        }
        async with self.client as client:
            await client.submit_log_forwarder_metrics(
                "test",
                [
                    {
                        "runtime_seconds": 2.80,
                        "resource_log_volume": {},
                        "timestamp": 1723040910,
                        "resource_log_bytes": {},
                    }
                ],
            )

        self.log.error.assert_called_once_with(
            "oops something went wrong",
            extra={"subscription_id": "decc348e-ca9e-4925-b351-ae56b0d9f811", "resource_group": "test_lfo"},
        )

    async def test_log_forwarder_container_created(self):
        async with self.client as client:
            await client.create_log_forwarder_containers(STORAGE_ACCOUNT_NAME)

        self.client.storage_client.blob_containers.create.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME, "dd-forwarder", AzureModelMatcher({})
        )

    async def test_storage_management_policy_creation(self):
        async with self.client as client:
            await client.create_log_forwarder_storage_management_policy(STORAGE_ACCOUNT_NAME)

        self.client.storage_client.management_policies.create_or_update.assert_awaited_once_with(
            RESOURCE_GROUP_NAME,
            STORAGE_ACCOUNT_NAME,
            "default",
            AzureModelMatcher(
                {
                    "policy": {
                        "rules": [
                            {
                                "enabled": True,
                                "name": "Delete Old Metric Blobs",
                                "type": "Lifecycle",
                                "definition": {
                                    "actions": {
                                        "base_blob": {"delete": {"days_after_modification_greater_than": 1}},
                                        "snapshot": {"delete": {"days_after_creation_greater_than": 1}},
                                    },
                                    "filters": {
                                        "blob_types": ["blockBlob", "appendBlob"],
                                    },
                                },
                            }
                        ]
                    }
                }
            ),
        )

    async def test_list_log_forwarder_ids_empty(self):
        self.client.container_apps_client.jobs.list_by_resource_group = Mock(return_value=async_generator())
        self.client.container_apps_client.managed_environments.list_by_resource_group = Mock(
            return_value=async_generator()
        )
        self.client.storage_client.storage_accounts.list_by_resource_group = Mock(return_value=async_generator())
        async with self.client as client:
            res = await client.list_log_forwarder_ids()
        self.assertEqual(res, set())

    async def test_list_log_forwarder_ids_all_same(self):
        self.client.container_apps_client.jobs.list_by_resource_group = Mock(
            return_value=async_generator(mock(name=get_container_app_name(CONFIG_ID1)))
        )
        self.client.container_apps_client.managed_environments.list_by_resource_group = Mock(
            return_value=async_generator(mock(name=get_managed_env_name(EAST_US, CONTROL_PLANE_ID)))
        )
        self.client.storage_client.storage_accounts.list_by_resource_group = Mock(
            return_value=async_generator(mock(name=get_storage_account_name(CONFIG_ID1)))
        )
        async with self.client as client:
            res = await client.list_log_forwarder_ids()
        self.assertEqual(res, {CONFIG_ID1})

    async def test_list_log_forwarder_ids_mixed(self):
        self.client.container_apps_client.jobs.list_by_resource_group = Mock(
            return_value=async_generator(
                mock(name=get_container_app_name(CONFIG_ID1)), mock(name=get_container_app_name(CONFIG_ID3))
            )
        )
        self.client.container_apps_client.managed_environments.list_by_resource_group = Mock(
            return_value=async_generator(
                mock(name=get_managed_env_name(EAST_US, CONTROL_PLANE_ID)),
                mock(name=get_storage_account_name(CONFIG_ID2)),
            )
        )
        self.client.storage_client.storage_accounts.list_by_resource_group = Mock(
            return_value=async_generator(
                mock(name=get_storage_account_name(CONFIG_ID2)), mock(name=get_storage_account_name(CONFIG_ID3))
            )
        )
        async with self.client as client:
            res = await client.list_log_forwarder_ids()
        self.assertEqual(res, {CONFIG_ID1, CONFIG_ID2, CONFIG_ID3})

    async def test_list_log_forwarder_ids_other_resources(self):
        self.client.container_apps_client.jobs.list_by_resource_group = Mock(
            return_value=async_generator(mock(name=get_container_app_name(CONFIG_ID1)), mock(name="other_job"))
        )
        self.client.container_apps_client.managed_environments.list_by_resource_group = Mock(
            return_value=async_generator(
                mock(name=get_managed_env_name(EAST_US, CONTROL_PLANE_ID)), mock(name="other_env")
            )
        )
        self.client.storage_client.storage_accounts.list_by_resource_group = Mock(
            return_value=async_generator(
                mock(name=get_storage_account_name("way_more_than_twelve_chars")), mock(name="storage_other")
            )
        )
        async with self.client as client:
            res = await client.list_log_forwarder_ids()
        self.assertEqual(res, {CONFIG_ID1, "way_more_than_twelve_chars"})

    async def test_create_log_forwarder_managed_env(self):
        # set up blob forwarder data
        (await self.container_client.download_blob()).content_as_bytes.return_value = b"some data"

        async with self.client:
            await self.client.create_log_forwarder_managed_environment(EAST_US, wait=True)

        # managed environment
        asp_create: AsyncMock = self.client.container_apps_client.managed_environments.begin_create_or_update
        asp_create.assert_awaited_once_with(
            RESOURCE_GROUP_NAME,
            MANAGED_ENV_EAST_US_NAME,
            AzureModelMatcher({"location": EAST_US, "zone_redundant": False}),
        )
        (await asp_create()).result.assert_awaited_once_with()

    async def test_create_log_forwarder_managed_env_in_unsupported_region(self):
        async with self.client:
            await self.client.create_log_forwarder_managed_environment(NEW_ZEALAND_NORTH)
        asp_create: AsyncMock = self.client.container_apps_client.managed_environments.begin_create_or_update
        asp_create.assert_awaited_once_with(
            RESOURCE_GROUP_NAME,
            MANAGED_ENV_EAST_US_NAME,
            AzureModelMatcher({"location": EAST_US, "zone_redundant": False}),
        )
        (await asp_create()).result.assert_not_called()

    async def test_get_forwarder_resources_both_exist(self):
        job, storage_account = Mock(name="job"), Mock(name="storage_account")
        self.client.container_apps_client.jobs.get.return_value = job
        self.client.container_apps_client.jobs.list_secrets.return_value = mock(value=[{"some_secret": "value"}])
        self.client.storage_client.storage_accounts.get_properties.return_value = storage_account
        async with self.client as client:
            res = await client.get_forwarder_resources(CONFIG_ID1)
        self.client.container_apps_client.jobs.get.assert_awaited_once_with(RESOURCE_GROUP_NAME, CONTAINER_APP_NAME)
        self.client.container_apps_client.jobs.list_secrets.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, CONTAINER_APP_NAME
        )
        self.client.storage_client.storage_accounts.get_properties.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME
        )
        self.assertEqual(res, (job, storage_account))
        self.assertEqual(res[0].configuration.secrets, [{"some_secret": "value"}])  # type: ignore

    async def test_get_forwarder_resources_neither_exist(self):
        self.client.container_apps_client.jobs.get.side_effect = ResourceNotFoundError()
        self.client.container_apps_client.jobs.list_secrets.return_value = mock(value=[{"some_secret": "value"}])
        self.client.storage_client.storage_accounts.get_properties.side_effect = ResourceNotFoundError()
        async with self.client as client:
            res = await client.get_forwarder_resources(CONFIG_ID1)
        self.client.container_apps_client.jobs.get.assert_awaited_once_with(RESOURCE_GROUP_NAME, CONTAINER_APP_NAME)
        self.client.container_apps_client.jobs.list_secrets.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, CONTAINER_APP_NAME
        )
        self.client.storage_client.storage_accounts.get_properties.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME
        )
        self.assertEqual(res, (None, None))

    async def test_get_forwarder_resources_job_not_found(self):
        storage_account = Mock(name="storage_account")
        self.client.container_apps_client.jobs.get.side_effect = ResourceNotFoundError()
        self.client.container_apps_client.jobs.list_secrets.return_value = mock(value=[{"some_secret": "value"}])
        self.client.storage_client.storage_accounts.get_properties.return_value = storage_account
        async with self.client as client:
            res = await client.get_forwarder_resources(CONFIG_ID1)
        self.client.container_apps_client.jobs.get.assert_awaited_once_with(RESOURCE_GROUP_NAME, CONTAINER_APP_NAME)
        self.client.container_apps_client.jobs.list_secrets.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, CONTAINER_APP_NAME
        )
        self.client.storage_client.storage_accounts.get_properties.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME
        )
        self.assertEqual(res, (None, storage_account))

    async def test_get_forwarder_resources_job_secrets_not_found(self):
        job, storage_account = Mock(name="job"), Mock(name="storage_account")
        job.configuration.secrets = []
        self.client.container_apps_client.jobs.get.return_value = job
        self.client.container_apps_client.jobs.list_secrets.side_effect = ResourceNotFoundError()
        self.client.storage_client.storage_accounts.get_properties.return_value = storage_account
        async with self.client as client:
            res = await client.get_forwarder_resources(CONFIG_ID1)
        self.client.container_apps_client.jobs.get.assert_awaited_once_with(RESOURCE_GROUP_NAME, CONTAINER_APP_NAME)
        self.client.container_apps_client.jobs.list_secrets.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, CONTAINER_APP_NAME
        )
        self.client.storage_client.storage_accounts.get_properties.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME
        )
        self.assertEqual(res, (job, storage_account))
        self.assertEqual(res[0].configuration.secrets, [])  # type: ignore

    async def test_get_forwarder_resources_storage_not_found(self):
        job = Mock(name="job")
        self.client.container_apps_client.jobs.get.return_value = job
        self.client.container_apps_client.jobs.list_secrets.return_value = mock(value=[{"some_secret": "value"}])
        self.client.storage_client.storage_accounts.get_properties.side_effect = ResourceNotFoundError()
        async with self.client as client:
            res = await client.get_forwarder_resources(CONFIG_ID1)
        self.client.container_apps_client.jobs.get.assert_awaited_once_with(RESOURCE_GROUP_NAME, CONTAINER_APP_NAME)
        self.client.container_apps_client.jobs.list_secrets.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, CONTAINER_APP_NAME
        )
        self.client.storage_client.storage_accounts.get_properties.assert_awaited_once_with(
            RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME
        )
        self.assertEqual(res, (job, None))
        self.assertEqual(res[0].configuration.secrets, [{"some_secret": "value"}])  # type: ignore
