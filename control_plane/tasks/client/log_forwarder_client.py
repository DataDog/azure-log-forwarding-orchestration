# stdlib
from asyncio import Lock, gather
from collections.abc import Awaitable
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timedelta
from logging import getLogger
from os import environ
from types import TracebackType
from typing import Self, TypeVar

# 3p
from aiohttp import ClientSession
from aiosonic.exceptions import RequestTimeout
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError, ServiceResponseTimeoutError
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.storage.v2023_05_01.aio import StorageManagementClient
from azure.mgmt.storage.v2023_05_01.models import (
    PublicNetworkAccess,
    Sku,
    StorageAccountCreateParameters,
    StorageAccountKey,
)
from azure.mgmt.web.v2023_12_01.aio import WebSiteManagementClient
from azure.mgmt.web.v2023_12_01.models import (
    AppServicePlan,
    ManagedServiceIdentity,
    NameValuePair,
    Site,
    SiteConfig,
    SkuDescription,
)
from azure.monitor.query import Metric, MetricsQueryResult, MetricValue
from azure.monitor.query.aio import MetricsQueryClient
from azure.storage.blob.aio import ContainerClient
from datadog_api_client import AsyncApiClient, Configuration
from datadog_api_client.v2.api.metrics_api import MetricsApi
from datadog_api_client.v2.model.metric_intake_type import MetricIntakeType
from datadog_api_client.v2.model.metric_payload import MetricPayload
from datadog_api_client.v2.model.metric_point import MetricPoint
from datadog_api_client.v2.model.metric_resource import MetricResource
from datadog_api_client.v2.model.metric_series import MetricSeries
from tenacity import RetryCallState, retry, stop_after_attempt

# project
from cache.common import (
    STORAGE_ACCOUNT_TYPE,
    LogForwarderType,
    get_app_service_plan_name,
    get_config_option,
    get_function_app_id,
    get_function_app_name,
    get_storage_account_name,
)
from tasks.task import wait_for_resource

BLOB_FORWARDER_DATA_CONTAINER, BLOB_FORWARDER_DATA_BLOB = "blob-forwarder", "data.zip"

COLLECTED_METRIC_DEFINITIONS = {"FunctionExecutionCount": "total"}

METRIC_COLLECTION_PERIOD_MINUTES = 30  # How long we are mointoring in minutes
METRIC_COLLECTION_SAMPLES = 6  # Number of samples we are collecting
METRIC_COLLECTION_GRANULARITY = METRIC_COLLECTION_PERIOD_MINUTES // METRIC_COLLECTION_SAMPLES

CLIENT_MAX_SECONDS_PER_METRIC = 5
CLIENT_MAX_SECONDS = CLIENT_MAX_SECONDS_PER_METRIC * len(COLLECTED_METRIC_DEFINITIONS)
MAX_ATTEMPS = 5


log = getLogger(__name__)

T = TypeVar("T")


async def ignore_exception_type(exc: type[BaseException], a: Awaitable[T]) -> T | None:
    try:
        return await a
    except exc:
        return None


async def is_exception_retryable(state: RetryCallState) -> bool:
    if (future := state.outcome) and (e := future.exception()):
        if isinstance(e, HttpResponseError):
            return e.status_code is not None and (e.status_code == 429 or e.status_code >= 500)
        if isinstance(e, RequestTimeout | ServiceResponseTimeoutError):
            return True
    return False


class LogForwarderClient(AbstractAsyncContextManager):
    def __init__(self, credential: DefaultAzureCredential, subscription_id: str, resource_group: str) -> None:
        self.control_plane_storage_connection_string = get_config_option("AzureWebJobsStorage")
        self._credential = credential
        self.web_client = WebSiteManagementClient(credential, subscription_id)
        self.storage_client = StorageManagementClient(credential, subscription_id)
        self.rest_client = ClientSession()
        self.monitor_client = MetricsQueryClient(credential)
        self.configuration = Configuration()
        self.configuration.request_timeout = CLIENT_MAX_SECONDS
        self.api_client = AsyncApiClient(self.configuration)
        self.api_instance = MetricsApi(self.api_client)
        self.resource_group = resource_group
        self._blob_forwarder_data_lock = Lock()
        self._blob_forwarder_data: bytes | None = None

    async def __aenter__(self) -> Self:
        await gather(
            self.web_client.__aenter__(),
            self.storage_client.__aenter__(),
            self.rest_client.__aenter__(),
            self.monitor_client.__aenter__(),
            self.api_client.__aenter__(),
        )
        token = await self._credential.get_token("https://management.azure.com/.default")
        self.rest_client.headers["Authorization"] = f"Bearer {token.token}"
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        await gather(
            self.web_client.__aexit__(exc_type, exc_val, exc_tb),
            self.storage_client.__aexit__(exc_type, exc_val, exc_tb),
            self.rest_client.__aexit__(exc_type, exc_val, exc_tb),
            self.monitor_client.__aexit__(exc_type, exc_val, exc_tb),
            self.api_client.__aexit__(exc_type, exc_val, exc_tb),
        )

    async def create_log_forwarder(self, region: str, config_id: str) -> LogForwarderType:
        storage_account_name = get_storage_account_name(config_id)
        app_service_plan_name = get_app_service_plan_name(config_id)
        log.info(
            "Creating log forwarder storage (%s) and app service plan (%s)",
            storage_account_name,
            app_service_plan_name,
        )
        # storage account
        storage_account_future = await self.storage_client.storage_accounts.begin_create(
            resource_group_name=self.resource_group,
            account_name=storage_account_name,
            parameters=StorageAccountCreateParameters(
                sku=Sku(
                    # TODO (AZINTS-2646): figure out which SKU we should be using here
                    name="Standard_LRS"
                ),
                kind="StorageV2",
                location=region,
                public_network_access=PublicNetworkAccess.DISABLED,
            ),
        )
        # app service plan
        app_service_plan_future = await self.web_client.app_service_plans.begin_create_or_update(
            self.resource_group,
            app_service_plan_name,
            AppServicePlan(
                location=region,
                kind="linux",
                reserved=True,
                sku=SkuDescription(
                    # TODO: figure out which SKU we should be using here
                    tier="Basic",
                    name="B1",
                ),
            ),
        )
        try:
            storage_account, app_service_plan = await gather(
                wait_for_resource(
                    storage_account_future,
                    lambda: self.storage_client.storage_accounts.get_properties(
                        self.resource_group, storage_account_name
                    ),
                ),
                wait_for_resource(
                    app_service_plan_future,
                    lambda: self.web_client.app_service_plans.get(self.resource_group, app_service_plan_name),
                ),
            )
            log.info(
                "Created log forwarder storage account (%s) and app service plan (%s)",
                storage_account.id,
                app_service_plan.id,
            )
        except Exception:
            log.exception("Failed to create storage account and/or app service plan")
            raise

        function_app_name = get_function_app_name(config_id)
        log.info("Creating log forwarder app: %s", function_app_name)
        connection_string = await self.get_connection_string(storage_account_name)
        function_app_poller = await self.web_client.web_apps.begin_create_or_update(
            self.resource_group,
            function_app_name,
            Site(
                location=region,
                kind="functionapp",
                identity=ManagedServiceIdentity(type="SystemAssigned"),
                server_farm_id=app_service_plan.id,
                https_only=True,
                site_config=SiteConfig(
                    app_settings=[
                        NameValuePair(name="FUNCTIONS_WORKER_RUNTIME", value="custom"),
                        NameValuePair(name="AzureWebJobsStorage", value=connection_string),
                        NameValuePair(name="FUNCTIONS_EXTENSION_VERSION", value="~4"),
                    ]
                ),
            ),
        )
        try:
            function_app, blob_forwarder_data = await gather(
                wait_for_resource(
                    function_app_poller, lambda: self.web_client.web_apps.get(self.resource_group, function_app_name)
                ),
                self.get_blob_forwarder_data(),
            )
        except Exception:
            log.exception("Failed to create function app and/or get blob forwarder data")
            raise
        log.info("Created log forwarder function app: %s", function_app.id)

        # deploy code to function app
        log.info("Deploying log forwarder code to function app: %s", function_app.id)
        resp = await self.rest_client.post(
            f"https://{function_app_name}.scm.azurewebsites.net/api/publish?type=zip",
            data=blob_forwarder_data,
        )
        resp.raise_for_status()
        body = await resp.text()
        log.info(
            "Deployed log forwarder code to function app (zip size %s)\nstatus code: %s\nbody: %s",
            len(blob_forwarder_data),
            resp.status,
            body,
        )

        # for now this is the only type we support
        return STORAGE_ACCOUNT_TYPE

    async def get_connection_string(self, storage_account_name: str) -> str:
        keys_result = await self.storage_client.storage_accounts.list_keys(self.resource_group, storage_account_name)
        keys: list[StorageAccountKey] = keys_result.keys  # type: ignore
        if len(keys) == 0:
            raise ValueError("No keys found for storage account")
        key: str = keys[0].value  # type: ignore
        return (
            "DefaultEndpointsProtocol=https;AccountName="
            + storage_account_name
            + ";AccountKey="
            + key
            + ";EndpointSuffix=core.windows.net"
        )

    async def get_blob_forwarder_data(self) -> bytes:
        async with self._blob_forwarder_data_lock:
            if self._blob_forwarder_data is None:
                async with ContainerClient.from_connection_string(
                    self.control_plane_storage_connection_string, BLOB_FORWARDER_DATA_CONTAINER
                ) as client:
                    stream = await client.download_blob(BLOB_FORWARDER_DATA_BLOB)
                    self._blob_forwarder_data = await stream.content_as_bytes(max_concurrency=4)
            return self._blob_forwarder_data

    async def delete_log_forwarder(self, forwarder_id: str, *, raise_error: bool = True, max_attempts: int = 3) -> bool:
        """Deletes the Log forwarder, returns True if successful, False otherwise"""

        @retry(stop=stop_after_attempt(max_attempts), retry=is_exception_retryable)
        async def _delete_forwarder():
            log.info("Attempting to delete log forwarder %s", forwarder_id)
            await gather(
                ignore_exception_type(
                    ResourceNotFoundError,
                    self.web_client.web_apps.delete(
                        self.resource_group, get_function_app_name(forwarder_id), delete_empty_server_farm=True
                    ),
                ),
                ignore_exception_type(
                    ResourceNotFoundError,
                    self.storage_client.storage_accounts.delete(
                        self.resource_group, get_storage_account_name(forwarder_id)
                    ),
                ),
            )
            log.info("Deleted log forwarder %s", forwarder_id)

        try:
            await _delete_forwarder()
            return True
        except Exception:
            if raise_error:
                raise
            return False

    @retry(retry=is_exception_retryable, stop=stop_after_attempt(MAX_ATTEMPS))
    async def get_log_forwarder_metrics(self, log_forwarder_id: str) -> MetricsQueryResult:
        return await self.monitor_client.query_resource(
            log_forwarder_id,
            metric_names=list(COLLECTED_METRIC_DEFINITIONS.keys()),
            timespan=timedelta(minutes=METRIC_COLLECTION_PERIOD_MINUTES),
            granularity=timedelta(minutes=METRIC_COLLECTION_GRANULARITY),
            timeout=CLIENT_MAX_SECONDS,
        )

    @retry(retry=is_exception_retryable, stop=stop_after_attempt(MAX_ATTEMPS))
    async def submit_log_forwarder_metrics(self, log_forwarder_id: str, metrics: list[Metric], sub_id: str) -> None:
        if "DD_API_KEY" not in environ:
            return
        metric_series: list[MetricSeries] = [self.create_metric_series(metric, log_forwarder_id) for metric in metrics]  # type: ignore
        if metric_series is None or not all(metric_series):
            log.warn(
                "Invalid timestamps for resource: %s, Skipping...",
                get_function_app_id(sub_id, self.resource_group, log_forwarder_id),
            )
            return
        body = MetricPayload(
            series=metric_series,
        )

        response = await self.api_instance.submit_metrics(body=body)  # type: ignore
        if len(response.get("errors", [])) > 0:
            for err in response.get("errors", []):
                log.error(err)

    def create_metric_series(self, metric: Metric, log_forwarder_id: str) -> MetricSeries | None:
        metric_points: list[MetricPoint | None] = [
            self.create_metric_point(metric_value, COLLECTED_METRIC_DEFINITIONS.get(metric.name))  # type: ignore
            for time_series_element in metric.timeseries
            for metric_value in time_series_element.data
        ]
        filtered_metric_points = [metric_point for metric_point in metric_points if metric_point is not None]
        if len(metric_points) == 0:
            return None
        return MetricSeries(  # type: ignore
            metric=metric.name,
            type=MetricIntakeType.UNSPECIFIED,
            points=filtered_metric_points,
            resources=[
                MetricResource(
                    name=get_function_app_name(log_forwarder_id),
                    type="logforwarder",
                ),
            ],
        )

    def create_metric_point(self, metric_value: MetricValue, metric_attr: str) -> MetricPoint | None:
        metric_timestamp = metric_value.timestamp.timestamp()
        if (datetime.now().timestamp() - metric_timestamp) > 3540:
            return None
        if getattr(metric_value, metric_attr, None) is None:
            return None
        return MetricPoint(  # type: ignore
            timestamp=int(metric_timestamp),
            value=getattr(metric_value, metric_attr),
        )
