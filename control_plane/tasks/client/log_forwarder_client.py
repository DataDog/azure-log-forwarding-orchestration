# stdlib
from asyncio import Lock, create_task, gather
from collections.abc import Awaitable, Callable, Iterable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime, timedelta
from logging import getLogger
from os import environ
from types import TracebackType
from typing import Any, Self, TypeAlias, TypeVar, cast

# 3p
from aiosonic.exceptions import RequestTimeout
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError, ServiceResponseTimeoutError
from azure.core.polling import AsyncLROPoller
from azure.identity.aio import DefaultAzureCredential
from azure.mgmt.appcontainers.aio import ContainerAppsAPIClient
from azure.mgmt.appcontainers.models import (
    Container,
    ContainerResources,
    EnvironmentVar,
    Job,
    JobConfiguration,
    JobConfigurationScheduleTriggerConfig,
    JobTemplate,
    ManagedEnvironment,
)
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
    ManagementPolicyName,
    ManagementPolicyRule,
    ManagementPolicySchema,
    ManagementPolicySnapShot,
    PublicNetworkAccess,
    Sku,
    StorageAccount,
    StorageAccountCreateParameters,
    StorageAccountKey,
)
from azure.storage.blob.aio import ContainerClient
from datadog_api_client import AsyncApiClient, Configuration
from datadog_api_client.v2.api.metrics_api import MetricsApi
from datadog_api_client.v2.model.intake_payload_accepted import IntakePayloadAccepted
from datadog_api_client.v2.model.metric_intake_type import MetricIntakeType
from datadog_api_client.v2.model.metric_payload import MetricPayload
from datadog_api_client.v2.model.metric_point import MetricPoint
from datadog_api_client.v2.model.metric_resource import MetricResource
from datadog_api_client.v2.model.metric_series import MetricSeries
from tenacity import RetryCallState, RetryError, retry, stop_after_attempt

# project
from cache.common import (
    CONTAINER_APP_PREFIX,
    MANAGED_ENVIRONMENT_PREFIX,
    STORAGE_ACCOUNT_PREFIX,
    STORAGE_ACCOUNT_TYPE,
    LogForwarderType,
    get_config_option,
    get_container_app_name,
    get_managed_env_id,
    get_managed_env_name,
    get_storage_account_name,
)
from cache.metric_blob_cache import MetricBlobEntry
from tasks.common import Resource, collect, wait_for_resource

FORWARDER_METRIC_CONTAINER_NAME = "dd-forwarder"

CLIENT_MAX_SECONDS = 5
MAX_ATTEMPS = 5

FORWARDER_METRIC_BLOB_LIFETIME_DAYS = 14

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


ResourcePoller: TypeAlias = tuple[AsyncLROPoller[T], Callable[[], Awaitable[T]]]


def get_datetime_str(time: datetime) -> str:
    return f"{time:%Y-%m-%d-%H}"


class LogForwarderClient(AbstractAsyncContextManager):
    def __init__(self, credential: DefaultAzureCredential, subscription_id: str, resource_group: str) -> None:
        self.forwarder_image = get_config_option("forwarder_image")
        self.dd_api_key = get_config_option("DD_API_KEY")
        self.should_submit_metrics = bool(environ.get("DD_APP_KEY") and environ.get("SHOULD_SUBMIT_METRICS"))
        self.resource_group = resource_group
        self.subscription_id = subscription_id
        self.container_apps_client = ContainerAppsAPIClient(credential, subscription_id)
        self.storage_client = StorageManagementClient(credential, subscription_id)
        self._datadog_client = AsyncApiClient(Configuration(request_timeout=CLIENT_MAX_SECONDS))
        self.metrics_client = MetricsApi(self._datadog_client)
        self._blob_forwarder_data_lock = Lock()
        self._blob_forwarder_data: bytes | None = None

    async def __aenter__(self) -> Self:
        await gather(
            self.container_apps_client.__aenter__(),
            self.storage_client.__aenter__(),
            self._datadog_client.__aenter__(),
        )
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        await gather(
            self.container_apps_client.__aexit__(exc_type, exc_val, exc_tb),
            self.storage_client.__aexit__(exc_type, exc_val, exc_tb),
            self._datadog_client.__aexit__(exc_type, exc_val, exc_tb),
        )

    def log_and_raise_errors(self, message: str, *maybe_errors: Any) -> None:
        errors = [e for e in maybe_errors if isinstance(e, Exception)]
        if errors:
            log.exception("%s: %s", message, errors)
            raise errors[0]

    async def create_log_forwarder(self, region: str, config_id: str) -> LogForwarderType:
        storage_account_name = get_storage_account_name(config_id)
        managed_env_name = get_managed_env_name(config_id)

        maybe_errors: tuple[Any, ...] = await gather(
            wait_for_resource(*await self.create_log_forwarder_storage_account(region, storage_account_name)),
            wait_for_resource(*await self.create_log_forwarder_managed_environment(region, managed_env_name)),
            return_exceptions=True,
        )
        self.log_and_raise_errors("Failed to create storage account and/or managed environment", *maybe_errors)

        maybe_errors = await gather(
            wait_for_resource(*await self.create_log_forwarder_container_app(region, config_id)),
            self.create_log_forwarder_containers(storage_account_name),
            self.create_log_forwarder_storage_management_policy(storage_account_name),
            return_exceptions=True,
        )
        self.log_and_raise_errors("Failed to create function app and/or get blob forwarder data", *maybe_errors)

        # for now this is the only type we support
        return STORAGE_ACCOUNT_TYPE

    async def create_log_forwarder_storage_account(
        self, region: str, storage_account_name: str
    ) -> ResourcePoller[StorageAccount]:
        log.info("Creating storage account %s for region %s", storage_account_name, region)
        return await self.storage_client.storage_accounts.begin_create(
            self.resource_group,
            storage_account_name,
            StorageAccountCreateParameters(
                sku=Sku(
                    # TODO (AZINTS-2646): figure out which SKU we should be using here
                    name="Standard_LRS"
                ),
                kind="StorageV2",
                location=region,
                public_network_access=PublicNetworkAccess.ENABLED,
            ),
        ), lambda: self.storage_client.storage_accounts.get_properties(self.resource_group, storage_account_name)

    async def create_log_forwarder_managed_environment(
        self, region: str, env_name: str
    ) -> ResourcePoller[ManagedEnvironment]:
        log.info("Creating managed environment %s for region %s", env_name, region)
        return await self.container_apps_client.managed_environments.begin_create_or_update(
            self.resource_group,
            env_name,
            ManagedEnvironment(
                location=region,
                zone_redundant=False,
            ),
        ), lambda: self.container_apps_client.managed_environments.get(self.resource_group, env_name)

    async def create_log_forwarder_container_app(self, region: str, config_id: str) -> ResourcePoller[Job]:
        connection_string = await self.get_connection_string(get_storage_account_name(config_id))
        job_name = get_container_app_name(config_id)
        return await self.container_apps_client.jobs.begin_create_or_update(
            self.resource_group,
            job_name,
            Job(
                location=region,
                environment_id=get_managed_env_id(self.subscription_id, self.resource_group, config_id),
                configuration=JobConfiguration(
                    trigger_type="Schedule",
                    schedule_trigger_config=JobConfigurationScheduleTriggerConfig(cron_expression="* * * * *"),
                    replica_timeout=1800,  # 30 minutes
                    replica_retry_limit=1,
                ),
                template=JobTemplate(
                    containers=[
                        Container(
                            name="forwarder",
                            image=self.forwarder_image,
                            resources=ContainerResources(cpu=0.5, memory="1Gi"),
                            env=[
                                EnvironmentVar(name="AzureWebJobsStorage", value=connection_string),
                                EnvironmentVar(name="DD_API_KEY", value=self.dd_api_key),
                            ],
                        )
                    ],
                ),
            ),
        ), lambda: self.container_apps_client.jobs.get(self.resource_group, job_name)

    async def create_log_forwarder_containers(self, storage_account_name: str) -> None:
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
            ManagementPolicyName.DEFAULT,
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
                                        delete=DateAfterModification(
                                            days_after_modification_greater_than=FORWARDER_METRIC_BLOB_LIFETIME_DAYS
                                        )
                                    ),
                                    snapshot=ManagementPolicySnapShot(
                                        delete=DateAfterCreation(
                                            days_after_creation_greater_than=FORWARDER_METRIC_BLOB_LIFETIME_DAYS
                                        )
                                    ),
                                ),
                                filters=ManagementPolicyFilter(
                                    blob_types=["blockBlob", "appendBlob"],
                                    prefix_match=[FORWARDER_METRIC_CONTAINER_NAME + "/"],
                                ),
                            ),
                        )
                    ]
                )
            ),
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

    async def delete_log_forwarder(self, forwarder_id: str, *, raise_error: bool = True, max_attempts: int = 3) -> bool:
        """Deletes the Log forwarder, returns True if successful, False otherwise"""

        @retry(stop=stop_after_attempt(max_attempts), retry=is_exception_retryable)
        async def _delete_forwarder():
            log.info("Attempting to delete log forwarder %s", forwarder_id)

            # start deleting the storage account now, it has no dependencies
            delete_storage_account_task = create_task(
                ignore_exception_type(
                    ResourceNotFoundError,
                    self.storage_client.storage_accounts.delete(
                        self.resource_group, get_storage_account_name(forwarder_id)
                    ),
                )
            )

            poller = await ignore_exception_type(
                ResourceNotFoundError,
                self.container_apps_client.jobs.begin_delete(self.resource_group, get_container_app_name(forwarder_id)),
            )
            if poller:
                await poller.result()

            poller = await self.container_apps_client.managed_environments.begin_delete(
                self.resource_group, get_managed_env_name(forwarder_id)
            )

            await gather(delete_storage_account_task, poller.result())
            log.info("Deleted log forwarder %s", forwarder_id)

        try:
            await _delete_forwarder()
            return True
        except Exception:
            if raise_error:
                raise
            return False

    async def get_blob_metrics_lines(self, config_id: str) -> list[str]:
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
            current_time: datetime = datetime.now(UTC)
            previous_hour: datetime = current_time - timedelta(hours=1)
            current_blob_name = f"metrics_{get_datetime_str(current_time)}.json"
            previous_blob_name = f"metrics_{get_datetime_str(previous_hour)}.json"
            results = await gather(
                *[
                    self.read_blob(container_client, previous_blob_name),
                    self.read_blob(container_client, current_blob_name),
                ],
                return_exceptions=True,
            )
            metric_lines: list[str] = []
            for result, blob in zip(results, [previous_blob_name, current_blob_name], strict=False):
                if isinstance(result, str):
                    metric_lines.extend(result.splitlines())
                else:
                    msg = ""
                    if isinstance(result, RetryError):
                        msg = "Max retries attempted, failed due to:\n"
                        result = result.last_attempt.exception() or "Unknown"
                    if isinstance(result, HttpResponseError):
                        msg += f"HttpResponseError with Response Code: {result.status_code}\nError: {result.error or result.reason or result.message}"
                    else:
                        msg += str(result)
                    log.error(
                        "Unable to fetch metrics in %s for forwarder %s:\n%s",
                        blob,
                        config_id,
                        msg,
                    )

            return metric_lines

    @retry(retry=is_exception_retryable, stop=stop_after_attempt(MAX_ATTEMPS))
    async def read_blob(self, container_client: ContainerClient, blob_name: str) -> str:
        try:
            async with container_client.get_blob_client(blob_name) as blob_client:
                raw_data = await blob_client.download_blob(timeout=CLIENT_MAX_SECONDS)
                dict_str = await raw_data.readall()
                return dict_str.decode("utf-8")
        except ResourceNotFoundError:
            return ""

    @retry(retry=is_exception_retryable, stop=stop_after_attempt(MAX_ATTEMPS))
    async def submit_log_forwarder_metrics(self, log_forwarder_id: str, metrics: list[MetricBlobEntry]) -> None:
        if not self.should_submit_metrics or not metrics:
            return

        response: IntakePayloadAccepted = await self.metrics_client.submit_metrics(
            body=self.create_metric_payload(metrics, log_forwarder_id)
        )  # type: ignore
        for error in response.get("errors", []):
            log.error(error)

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
                                name=get_container_app_name(log_forwarder_id),
                                type="logforwarder",
                            ),
                        ],
                    ),
                ]
            ),
        )

    async def list_log_forwarder_ids(self) -> set[str]:
        jobs, envs, storage_accounts = await gather(
            collect(self.container_apps_client.jobs.list_by_resource_group(self.resource_group)),
            collect(self.container_apps_client.managed_environments.list_by_resource_group(self.resource_group)),
            collect(self.storage_client.storage_accounts.list_by_resource_group(self.resource_group)),
        )

        def _get_forwarder_config_ids(it: Iterable[Resource], prefix: str) -> set[str]:
            return {resource.name.removeprefix(prefix) for resource in it if resource.name.startswith(prefix)}

        return (
            _get_forwarder_config_ids(cast(Iterable[Resource], jobs), CONTAINER_APP_PREFIX)
            | _get_forwarder_config_ids(cast(Iterable[Resource], envs), MANAGED_ENVIRONMENT_PREFIX)
            | _get_forwarder_config_ids(cast(Iterable[Resource], storage_accounts), STORAGE_ACCOUNT_PREFIX)
        )
