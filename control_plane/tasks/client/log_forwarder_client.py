# stdlib
from asyncio import Lock, create_task, gather
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime, timedelta
from logging import getLogger
from os import environ
from types import TracebackType
from typing import Self, TypeVar, cast

# 3p
from aiohttp import ClientSession
from aiosonic.exceptions import RequestTimeout
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError, ServiceResponseTimeoutError
from azure.core.polling import AsyncLROPoller
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.storage.v2023_05_01.aio import StorageManagementClient
from azure.mgmt.storage.v2023_05_01.models import (
    BlobContainer,
    DateAfterCreation,
    DateAfterModification,
    ManagementPolicy,
    ManagementPolicyAction,
    ManagementPolicyBaseBlob,
    ManagementPolicyDefinition,
    ManagementPolicyFilter,
    ManagementPolicyRule,
    ManagementPolicySchema,
    ManagementPolicySnapShot,
    PublicNetworkAccess,
    Sku,
    StorageAccount,
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
from azure.storage.blob.aio import ContainerClient
from datadog_api_client import AsyncApiClient, Configuration
from datadog_api_client.v2.api.metrics_api import IntakePayloadAccepted, MetricsApi
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
    get_function_app_name,
    get_storage_account_name,
)
from cache.metric_blob_cache import MetricBlobEntry
from tasks.common import wait_for_resource

BLOB_FORWARDER_DATA_CONTAINER, BLOB_FORWARDER_DATA_BLOB = "blob-forwarder", "data.zip"

FORWARDER_METRIC_CONTAINER_NAME = "forwarder-metrics"

CLIENT_MAX_SECONDS = 5
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

        blob_forwarder_data_task = create_task(self.get_blob_forwarder_data())

        try:
            storage_account, app_service_plan = await gather(
                wait_for_resource(*await self.create_log_forwarder_storage_account(region, storage_account_name)),
                wait_for_resource(*await self.create_log_forwarder_app_service_plan(region, app_service_plan_name)),
            )
            log.info(
                "Created log forwarder storage account (%s) and app service plan (%s)",
                storage_account.id,
                app_service_plan.id,
            )
        except Exception:
            log.exception("Failed to create storage account and/or app service plan")
            raise

        create_containers_task = create_task(self.create_log_forwarder_containers(storage_account_name))
        create_management_policy_task = create_task(
            self.create_log_forwarder_storage_management_policy(storage_account_name)
        )

        function_app_name = get_function_app_name(config_id)
        log.info("Creating log forwarder app: %s", function_app_name)
        try:
            function_app, blob_forwarder_data = await gather(
                wait_for_resource(
                    *await self.create_log_forwarder_function_app(
                        region, function_app_name, cast(str, app_service_plan.id), storage_account_name
                    )
                ),
                blob_forwarder_data_task,
            )
        except Exception:
            log.exception("Failed to create function app and/or get blob forwarder data")
            raise
        log.info("Created log forwarder function app: %s", function_app.id)

        await gather(
            self.deploy_log_forwarder_function_app(function_app_name, blob_forwarder_data),
            create_containers_task,
            create_management_policy_task,
        )

        # for now this is the only type we support
        return STORAGE_ACCOUNT_TYPE

    async def create_log_forwarder_storage_account(
        self, region: str, storage_account_name: str
    ) -> tuple[AsyncLROPoller[StorageAccount], Callable[[], Awaitable[StorageAccount]]]:
        return await self.storage_client.storage_accounts.begin_create(
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
        ), lambda: self.storage_client.storage_accounts.get_properties(self.resource_group, storage_account_name)

    async def create_log_forwarder_app_service_plan(
        self, region: str, app_service_plan_name: str
    ) -> tuple[AsyncLROPoller[AppServicePlan], Callable[[], Awaitable[AppServicePlan]]]:
        return await self.web_client.app_service_plans.begin_create_or_update(
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
        ), lambda: self.web_client.app_service_plans.get(self.resource_group, app_service_plan_name)

    async def create_log_forwarder_containers(self, storage_account_name: str):
        await self.storage_client.blob_containers.create(
            self.resource_group,
            storage_account_name,
            FORWARDER_METRIC_CONTAINER_NAME,
            BlobContainer(),
        )

    async def create_log_forwarder_storage_management_policy(self, storage_account_name: str) -> None:
        await self.storage_client.management_policies.create_or_update(
            self.resource_group,
            storage_account_name,
            "default",  # required to be "default" because of the API
            ManagementPolicy(
                policy=ManagementPolicySchema(
                    rules=[
                        ManagementPolicyRule(
                            enabled=True,
                            name="Delete Old Metric Blobs",
                            type="Lifecycle",
                            definition=ManagementPolicyDefinition(
                                actions=ManagementPolicyAction(
                                    base_blob=ManagementPolicyBaseBlob(
                                        delete=DateAfterModification(days_after_modification_greater_than=14)
                                    ),
                                    snapshot=ManagementPolicySnapShot(
                                        delete=DateAfterCreation(days_after_creation_greater_than=14)
                                    ),
                                ),
                                filters=ManagementPolicyFilter(
                                    blob_types=["blockBlob", "appendBlob"], prefix_match=["forwarder-metrics/"]
                                ),
                            ),
                        )
                    ]
                )
            ),
        )

    async def create_log_forwarder_function_app(
        self, region: str, function_app_name: str, app_service_plan_id: str, storage_account_name: str
    ) -> tuple[AsyncLROPoller[Site], Callable[[], Awaitable[Site]]]:
        connection_string = await self.get_connection_string(storage_account_name)
        return await self.web_client.web_apps.begin_create_or_update(
            self.resource_group,
            function_app_name,
            Site(
                location=region,
                kind="functionapp",
                identity=ManagedServiceIdentity(type="SystemAssigned"),
                server_farm_id=app_service_plan_id,
                https_only=True,
                site_config=SiteConfig(
                    app_settings=[
                        NameValuePair(name="FUNCTIONS_WORKER_RUNTIME", value="custom"),
                        NameValuePair(name="AzureWebJobsStorage", value=connection_string),
                        NameValuePair(name="FUNCTIONS_EXTENSION_VERSION", value="~4"),
                    ]
                ),
            ),
        ), lambda: self.web_client.web_apps.get(self.resource_group, function_app_name)

    async def deploy_log_forwarder_function_app(self, function_app_name: str, blob_forwarder_data: bytes) -> None:
        log.info("Deploying log forwarder code to function app: %s", function_app_name)
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

    async def get_blob_metrics(self, config_id: str) -> list[str]:
        """
        Returns a list of json decodable strings that represent metrics
        json string takes form of {'Values': [metric_dict]}
        metric_dict is as follows {'Name': str, 'Value': float, 'Time': float}
        Time is a unix timestamp
        """
        conn_str = await self.get_connection_string(get_storage_account_name(config_id))
        async with ContainerClient.from_connection_string(
            conn_str, FORWARDER_METRIC_CONTAINER_NAME
        ) as container_client:
            metrics = []
            current_time: datetime = datetime.now(UTC)
            previous_hour: datetime = current_time - timedelta(hours=1)
            current_blob_name = f"{self.get_datetime_str(current_time)}.txt"
            previous_blob_name = f"{self.get_datetime_str(previous_hour)}.txt"
            results = await gather(
                *[
                    self.read_blob(container_client, previous_blob_name),
                    self.read_blob(container_client, current_blob_name),
                ]
            )
            for result in results:
                metrics.extend(result)
            return metrics

    @retry(retry=is_exception_retryable, stop=stop_after_attempt(MAX_ATTEMPS))
    async def read_blob(self, container_client: ContainerClient, blob_name: str) -> list[str]:
        try:
            async with container_client.get_blob_client(blob_name) as blob_client:
                raw_data = await blob_client.download_blob(timeout=CLIENT_MAX_SECONDS)
                dict_str = await raw_data.readall()
                return dict_str.decode("utf-8").split("\n")
        except ResourceNotFoundError:
            return []

    @retry(retry=is_exception_retryable, stop=stop_after_attempt(MAX_ATTEMPS))
    async def submit_log_forwarder_metrics(self, log_forwarder_id: str, metrics: list[MetricBlobEntry]) -> None:
        if not metrics or not environ.get("SHOULD_SUBMIT_METRICS", False) or not environ.get("DD_API_KEY"):
            return

        response: IntakePayloadAccepted = await self.api_instance.submit_metrics(
            body=self.create_metric_payload(metrics, log_forwarder_id)
        )  # type: ignore
        for error in response.get("errors", []):
            log.error(error)

    def get_datetime_str(self, time: datetime) -> str:
        return f"{time:%Y-%m-%d-%H}"

    def create_metric_payload(self, metric_entries: list[MetricBlobEntry], log_forwarder_id: str) -> MetricPayload:
        return cast(  # annoying hack to get mypy typing to work since the SDK overrides __new__
            MetricPayload,
            MetricPayload(
                series=[
                    MetricSeries(
                        metric="Runtime",
                        type=MetricIntakeType.UNSPECIFIED,
                        points=[
                            MetricPoint(
                                timestamp=int(metric["timestamp"]),
                                value=metric["runtime_seconds"],
                            )
                            for metric in metric_entries
                        ],
                        resources=[
                            MetricResource(
                                name=get_function_app_name(log_forwarder_id),
                                type="logforwarder",
                            ),
                        ],
                    ),
                ]
            ),
        )
