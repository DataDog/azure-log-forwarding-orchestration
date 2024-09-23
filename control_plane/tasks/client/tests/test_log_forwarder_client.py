# stdlib
from os import environ
from unittest.mock import ANY, DEFAULT, AsyncMock, MagicMock, Mock

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
from cache.common import (
    CONTAINER_APP_PREFIX,
    MANAGED_ENVIRONMENT_PREFIX,
    STORAGE_ACCOUNT_PREFIX,
    get_container_app_name,
    get_managed_env_name,
    get_storage_account_name,
)
from cache.metric_blob_cache import MetricBlobEntry
from tasks.client.log_forwarder_client import MAX_ATTEMPS, LogForwarderClient
from tasks.tests.common import AsyncMockClient, AsyncTestCase, AzureModelMatcher, async_generator, mock

sub_id1 = "decc348e-ca9e-4925-b351-ae56b0d9f811"
EAST_US = "eastus"
WEST_US = "westus"
config_id = "d6fc2c757f9c"
config_id2 = "e8d5222d1c46"
config_id3 = "619fff16cae1"
managed_env_name = MANAGED_ENVIRONMENT_PREFIX + config_id
container_app_name = CONTAINER_APP_PREFIX + config_id
storage_account_name = STORAGE_ACCOUNT_PREFIX + config_id
rg1 = "test_lfo"


class FakeHttpError(HttpResponseError):
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    @property
    def message(self):
        return {"code": f"something related to {self.status_code}"}

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


class TestLogForwarderClient(AsyncTestCase):
    async def asyncSetUp(self) -> None:
        environ.clear()
        self.addCleanup(environ.clear)
        environ["AzureWebJobsStorage"] = "..."
        environ["DD_API_KEY"] = "123123"
        environ["DD_APP_KEY"] = "456456"
        environ["DD_SITE"] = "datadoghq.com"
        environ["SHOULD_SUBMIT_METRICS"] = ""
        environ["forwarder_image"] = "ddlfo.azurecr.io/blobforwarder:latest"

        self.client: MockedLogForwarderClient = LogForwarderClient(  # type: ignore
            credential=AsyncMock(), subscription_id=sub_id1, resource_group=rg1
        )
        await self.client.__aexit__(None, None, None)
        self.client.container_apps_client = AsyncMock()
        self.client.storage_client = AsyncMock()
        self.client._datadog_client = AsyncMock()
        self.client.metrics_client = AsyncMock()
        self.client.storage_client.storage_accounts.list_keys = AsyncMock(return_value=Mock(keys=[Mock(value="key")]))
        self.container_client_class = self.patch_path("tasks.client.log_forwarder_client.ContainerClient")

        self.log = self.patch_path("tasks.client.log_forwarder_client.log")

        self.container_client = AsyncMockClient()
        self.container_client_class.from_connection_string.return_value = self.container_client
        self.container_client.get_blob_client = MagicMock()
        self.blob_client = AsyncMockClient()
        self.container_client.get_blob_client.return_value = self.blob_client

    async def test_create_log_forwarder(self):
        # set up blob forwarder data
        (await self.container_client.download_blob()).content_as_bytes.return_value = b"some data"

        async with self.client:
            await self.client.create_log_forwarder(EAST_US, config_id)

        # managed environment
        asp_create: AsyncMock = self.client.container_apps_client.managed_environments.begin_create_or_update
        asp_create.assert_awaited_once_with(rg1, managed_env_name, ANY)
        (await asp_create()).result.assert_awaited_once_with()
        # storage account
        storage_create: AsyncMock = self.client.storage_client.storage_accounts.begin_create
        storage_create.assert_awaited_once_with(rg1, storage_account_name, ANY)
        (await storage_create()).result.assert_awaited_once_with()
        # function app
        function_create: AsyncMock = self.client.container_apps_client.jobs.begin_create_or_update
        function_create.assert_awaited_once_with(rg1, container_app_name, ANY)
        (await function_create()).result.assert_awaited_once_with()

    async def test_create_log_forwarder_no_keys(self):
        self.client.storage_client.storage_accounts.list_keys = AsyncMock(return_value=Mock(keys=[]))
        with self.assertRaises(ValueError) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US, config_id)
        self.assertIn("No keys found for storage account", str(ctx.exception))

    async def test_create_log_forwarder_managed_env_failure(self):
        (
            await self.client.container_apps_client.managed_environments.begin_create_or_update()
        ).result.side_effect = Exception("400: ASP creation failed")
        with self.assertRaises(Exception) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US, config_id)
        self.assertIn("400: ASP creation failed", str(ctx.exception))

    async def test_create_log_forwarder_storage_account_failure(self):
        (await self.client.storage_client.storage_accounts.begin_create()).result.side_effect = Exception(
            "400: Storage Account creation failed"
        )
        with self.assertRaises(Exception) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US, config_id)
        self.assertIn("400: Storage Account creation failed", str(ctx.exception))

    async def test_create_log_forwarder_container_app_failure(self):
        (await self.client.container_apps_client.jobs.begin_create_or_update()).result.side_effect = Exception(
            "400: Function App creation failed"
        )
        with self.assertRaises(Exception) as ctx:
            async with self.client:
                await self.client.create_log_forwarder(EAST_US, config_id)
        self.assertIn("400: Function App creation failed", str(ctx.exception))

    async def test_delete_log_forwarder(self):
        async with self.client as client:
            success = await client.delete_log_forwarder(config_id)
        self.assertTrue(success)
        self.client.container_apps_client.jobs.begin_delete.assert_awaited_once_with(rg1, container_app_name)
        self.client.storage_client.storage_accounts.delete.assert_awaited_once_with(rg1, storage_account_name)

    async def test_delete_log_forwarder_ignore_resource_not_found(self):
        self.client.container_apps_client.jobs.begin_delete.side_effect = ResourceNotFoundError()
        self.client.storage_client.storage_accounts.delete.side_effect = ResourceNotFoundError()
        async with self.client as client:
            success = await client.delete_log_forwarder(config_id)
        self.assertTrue(success)
        self.client.container_apps_client.jobs.begin_delete.assert_awaited_once_with(rg1, container_app_name)
        self.client.storage_client.storage_accounts.delete.assert_awaited_once_with(rg1, storage_account_name)

    async def test_delete_log_forwarder_not_raise_error(self):
        self.client.container_apps_client.jobs.begin_delete.side_effect = FakeHttpError(400)
        async with self.client as client:
            success = await client.delete_log_forwarder(config_id, raise_error=False)
        self.assertFalse(success)
        self.client.container_apps_client.jobs.begin_delete.assert_awaited_once_with(rg1, container_app_name)
        self.client.storage_client.storage_accounts.delete.assert_awaited_once_with(rg1, storage_account_name)

    async def test_delete_log_forwarder_makes_3_retryable_attempts_default(self):
        self.client.container_apps_client.jobs.begin_delete.side_effect = FakeHttpError(429)
        with self.assertRaises(RetryError) as ctx:
            async with self.client as client:
                await client.delete_log_forwarder(config_id)
        self.assertEqual(ctx.exception.last_attempt.exception(), FakeHttpError(429))
        self.assertCalledTimesWith(self.client.container_apps_client.jobs.begin_delete, 3, rg1, container_app_name)
        self.assertCalledTimesWith(self.client.storage_client.storage_accounts.delete, 3, rg1, storage_account_name)

    async def test_delete_log_forwarder_makes_5_retryable_attempts(self):
        self.client.container_apps_client.jobs.begin_delete.side_effect = FakeHttpError(529)
        self.client.storage_client.storage_accounts.delete.side_effect = ResourceNotFoundError()
        with self.assertRaises(RetryError) as ctx:
            async with self.client as client:
                await client.delete_log_forwarder(config_id, max_attempts=5)
        self.assertEqual(ctx.exception.last_attempt.exception(), FakeHttpError(529))
        self.assertCalledTimesWith(self.client.container_apps_client.jobs.begin_delete, 5, rg1, container_app_name)
        self.assertCalledTimesWith(self.client.storage_client.storage_accounts.delete, 5, rg1, storage_account_name)

    async def test_delete_log_forwarder_doesnt_retry_after_second_unretryable(self):
        self.client.container_apps_client.jobs.begin_delete.side_effect = [FakeHttpError(429), FakeHttpError(400)]
        self.client.storage_client.storage_accounts.delete.side_effect = ResourceNotFoundError()

        with self.assertRaises(FakeHttpError) as ctx:
            async with self.client as client:
                await client.delete_log_forwarder(config_id)
        self.assertEqual(ctx.exception, FakeHttpError(400))
        self.assertCalledTimesWith(self.client.container_apps_client.jobs.begin_delete, 2, rg1, container_app_name)
        self.assertCalledTimesWith(self.client.storage_client.storage_accounts.delete, 2, rg1, storage_account_name)

    async def test_get_blob_metrics_standard_execution(self):
        (await self.blob_client.download_blob()).readall.return_value = b"hi\nbye"

        async with self.client as client:
            res = await client.get_blob_metrics_lines("test")
            self.assertEqual(res, ["hi", "bye", "hi", "bye"])

    async def test_get_blob_metrics_missing_blob(self):
        (await self.blob_client.download_blob()).readall.return_value = b"hi\nbye"
        self.blob_client.download_blob.side_effect = [ResourceNotFoundError(), DEFAULT]

        async with self.client as client:
            res = await client.get_blob_metrics_lines("test")
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
            res = await client.get_blob_metrics_lines("test")
            self.assertEqual(res, ["hi", "bye", "hi", "bye"])
            self.assertEqual(self.blob_client.download_blob.call_count, 6)  # 1 call is from where res_str is set

    async def test_get_blob_max_retries(self):
        self.blob_client.download_blob.side_effect = ServiceResponseTimeoutError("oops")

        async with self.client as client:
            await client.get_blob_metrics_lines("test")
        self.assertEqual(self.blob_client.download_blob.call_count, 2 * MAX_ATTEMPS)
        self.log.error.assert_called_with(
            "Unable to fetch metrics in %s for forwarder %s:\n%s",
            ANY,
            "test",
            "Max retries attempted, failed due to:\noops",
        )

    async def test_get_blob_unretryable_exception(self):
        self.blob_client.download_blob.side_effect = FakeHttpError(402)

        async with self.client as client:
            await client.get_blob_metrics_lines("test")
        self.assertEqual(self.blob_client.download_blob.call_count, 2)
        self.log.error.assert_called_with(
            "Unable to fetch metrics in %s for forwarder %s:\n%s",
            ANY,
            "test",
            "HttpResponseError with Response Code: 402\nError: {'code': 'something related to 402'}",
        )

    async def test_submit_metrics_normal_execution(self):
        self.client.should_submit_metrics = True
        sample_metric_entry_list: list[MetricBlobEntry] = [
            {
                "timestamp": 1723040910,
                "runtime_seconds": 280,
                "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 6},
            },
            {
                "timestamp": 1723040911,
                "runtime_seconds": 281,
                "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 6},
            },
        ]
        self.client.metrics_client.submit_metrics.return_value = {}
        sample_body = MetricPayload(
            series=[
                MetricSeries(
                    metric="Runtime",
                    type=MetricIntakeType.UNSPECIFIED,
                    points=[
                        MetricPoint(
                            timestamp=1723040910,
                            value=280,
                        ),
                        MetricPoint(
                            timestamp=1723040911,
                            value=281,
                        ),
                    ],
                    resources=[
                        MetricResource(
                            name=get_container_app_name("test"),
                            type="logforwarder",
                        ),
                    ],
                )
            ],
        )
        async with self.client as client:
            await client.submit_log_forwarder_metrics("test", sample_metric_entry_list)

        self.client.metrics_client.submit_metrics.assert_called_once_with(body=sample_body)

    async def test_submit_metrics_retries(self):
        self.client.should_submit_metrics = True
        sample_metric_entry_list: list[MetricBlobEntry] = [
            {
                "timestamp": 1723040910,
                "runtime_seconds": 2.80,
                "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 6},
            },
            {
                "timestamp": 1723040911,
                "runtime_seconds": 2.81,
                "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 6},
            },
        ]
        self.client.metrics_client.submit_metrics.side_effect = [RequestTimeout(), RequestTimeout(), DEFAULT]
        self.client.metrics_client.submit_metrics.return_value = {}
        sample_body = MetricPayload(
            series=[
                MetricSeries(
                    metric="Runtime",
                    type=MetricIntakeType.UNSPECIFIED,
                    points=[
                        MetricPoint(
                            timestamp=1723040910,
                            value=2.80,
                        ),
                        MetricPoint(
                            timestamp=1723040911,
                            value=2.81,
                        ),
                    ],
                    resources=[
                        MetricResource(
                            name=get_container_app_name("test"),
                            type="logforwarder",
                        ),
                    ],
                )
            ],
        )
        async with self.client as client:
            await client.submit_log_forwarder_metrics("test", sample_metric_entry_list)

        self.client.metrics_client.submit_metrics.assert_called_with(body=sample_body)
        self.assertEqual(self.client.metrics_client.submit_metrics.call_count, 3)

    async def test_submit_metrics_max_retries(self):
        self.client.should_submit_metrics = True
        sample_metric_entry_list: list[MetricBlobEntry] = [
            {
                "timestamp": 1723040910,
                "runtime_seconds": 2.80,
                "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 6},
            },
            {
                "timestamp": 1723040911,
                "runtime_seconds": 2.81,
                "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 6},
            },
        ]
        self.client.metrics_client.submit_metrics.side_effect = RequestTimeout()
        sample_body = MetricPayload(
            series=[
                MetricSeries(
                    metric="Runtime",
                    type=MetricIntakeType.UNSPECIFIED,
                    points=[
                        MetricPoint(
                            timestamp=1723040910,
                            value=2.80,
                        ),
                        MetricPoint(
                            timestamp=1723040911,
                            value=2.81,
                        ),
                    ],
                    resources=[
                        MetricResource(
                            name=get_container_app_name("test"),
                            type="logforwarder",
                        ),
                    ],
                )
            ],
        )
        with self.assertRaises(RetryError) as ctx:
            async with self.client as client:
                await client.submit_log_forwarder_metrics("test", sample_metric_entry_list)
        self.client.metrics_client.submit_metrics.assert_called_with(body=sample_body)
        self.assertEqual(self.client.metrics_client.submit_metrics.call_count, MAX_ATTEMPS)

        self.assertIsInstance(ctx.exception.last_attempt.exception(), RequestTimeout)

    async def test_submit_metrics_nonretryable_exception(self):
        self.client.should_submit_metrics = True
        sample_metric_entry_list: list[MetricBlobEntry] = [
            {
                "timestamp": 1723040910,
                "runtime_seconds": 2.80,
                "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 6},
            },
            {
                "timestamp": 1723040911,
                "runtime_seconds": 2.81,
                "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 6},
            },
        ]
        self.client.metrics_client.submit_metrics.side_effect = FakeHttpError(404)
        self.client.metrics_client.submit_metrics.return_value = {}
        sample_body = MetricPayload(
            series=[
                MetricSeries(
                    metric="Runtime",
                    type=MetricIntakeType.UNSPECIFIED,
                    points=[
                        MetricPoint(
                            timestamp=1723040910,
                            value=2.80,
                        ),
                        MetricPoint(
                            timestamp=1723040911,
                            value=2.81,
                        ),
                    ],
                    resources=[
                        MetricResource(
                            name=get_container_app_name("test"),
                            type="logforwarder",
                        ),
                    ],
                )
            ],
        )
        with self.assertRaises(FakeHttpError):
            async with self.client as client:
                await client.submit_log_forwarder_metrics("test", sample_metric_entry_list)
        self.client.metrics_client.submit_metrics.assert_called_with(body=sample_body)
        self.assertEqual(self.client.metrics_client.submit_metrics.call_count, 1)

    async def test_submit_metrics_errors_logged(self):
        self.client.should_submit_metrics = True
        self.client.metrics_client.submit_metrics.return_value = {
            "errors": [
                "oops something went wrong",
            ]
        }
        async with self.client as client:
            await client.submit_log_forwarder_metrics(
                "test", [{"runtime_seconds": 2.80, "resource_log_volume": {}, "timestamp": 1723040910}]
            )

        self.log.error.assert_called_once_with("oops something went wrong")

    async def test_log_forwarder_container_created(self):
        async with self.client as client:
            await client.create_log_forwarder_containers(storage_account_name)

        self.client.storage_client.blob_containers.create.assert_awaited_once_with(
            rg1, storage_account_name, "dd-forwarder", ANY
        )

    async def test_storage_management_policy_creation(self):
        async with self.client as client:
            await client.create_log_forwarder_storage_management_policy(storage_account_name)

        self.client.storage_client.management_policies.create_or_update.assert_awaited_once_with(
            rg1,
            storage_account_name,
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
                                        "base_blob": {"delete": {"days_after_modification_greater_than": 14}},
                                        "snapshot": {"delete": {"days_after_creation_greater_than": 14}},
                                    },
                                    "filters": {
                                        "prefix_match": ["dd-forwarder/"],
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
            return_value=async_generator(mock(name=get_container_app_name(config_id)))
        )
        self.client.container_apps_client.managed_environments.list_by_resource_group = Mock(
            return_value=async_generator(mock(name=get_managed_env_name(config_id)))
        )
        self.client.storage_client.storage_accounts.list_by_resource_group = Mock(
            return_value=async_generator(mock(name=get_storage_account_name(config_id)))
        )
        async with self.client as client:
            res = await client.list_log_forwarder_ids()
        self.assertEqual(res, {config_id})

    async def test_list_log_forwarder_ids_mixed(self):
        self.client.container_apps_client.jobs.list_by_resource_group = Mock(
            return_value=async_generator(
                mock(name=get_container_app_name(config_id)), mock(name=get_container_app_name(config_id3))
            )
        )
        self.client.container_apps_client.managed_environments.list_by_resource_group = Mock(
            return_value=async_generator(
                mock(name=get_managed_env_name(config_id)), mock(name=get_storage_account_name(config_id2))
            )
        )
        self.client.storage_client.storage_accounts.list_by_resource_group = Mock(
            return_value=async_generator(
                mock(name=get_storage_account_name(config_id2)), mock(name=get_storage_account_name(config_id3))
            )
        )
        async with self.client as client:
            res = await client.list_log_forwarder_ids()
        self.assertEqual(res, {config_id, config_id2, config_id3})

    async def test_list_log_forwarder_ids_other_resources(self):
        self.client.container_apps_client.jobs.list_by_resource_group = Mock(
            return_value=async_generator(mock(name=get_container_app_name(config_id)), mock(name="other_job"))
        )
        self.client.container_apps_client.managed_environments.list_by_resource_group = Mock(
            return_value=async_generator(mock(name=get_managed_env_name(config_id)), mock(name="other_env"))
        )
        self.client.storage_client.storage_accounts.list_by_resource_group = Mock(
            return_value=async_generator(
                mock(name=get_storage_account_name("way_more_than_twelve_chars")), mock(name="storage_other")
            )
        )
        async with self.client as client:
            res = await client.list_log_forwarder_ids()
        self.assertEqual(res, {config_id, "way_more_than_twelve_chars"})
