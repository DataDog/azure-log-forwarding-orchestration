# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from asyncio import Lock, Task as AsyncTask, create_task, gather, wait
from collections.abc import Awaitable, Callable, Coroutine, Iterable
from contextlib import AbstractAsyncContextManager, suppress
from datetime import UTC, datetime, timedelta
from logging import Logger
from types import TracebackType
from typing import Any, Literal, Self, TypeAlias, TypeVar, cast

# 3p
from aiosonic.exceptions import RequestTimeout
from azure.core.exceptions import (
    HttpResponseError,
    ResourceNotFoundError,
    ServiceResponseTimeoutError,
)
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
    Secret,
)
from azure.mgmt.storage.v2024_01_01.aio import StorageManagementClient
from azure.mgmt.storage.v2024_01_01.models import (
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
from azure.storage.blob.aio import ContainerClient, StorageStreamDownloader
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
    STORAGE_ACCOUNT_TYPE,
    LogForwarderType,
)
from cache.env import (
    CONFIG_ID_SETTING,
    CONNECTION_STRING_SECRET,
    CONTROL_PLANE_ID_SETTING,
    CONTROL_PLANE_REGION_SETTING,
    DD_API_KEY_SECRET,
    DD_API_KEY_SETTING,
    DD_SITE_SETTING,
    DD_TELEMETRY_SETTING,
    FORWARDER_IMAGE_SETTING,
    PII_SCRUBBER_RULES_SETTING,
    STORAGE_CONNECTION_SETTING,
    get_config_option,
    is_truthy,
)
from cache.metric_blob_cache import (
    METRIC_NAMES,
    MetricBlobEntry,
    deserialize_blob_metric_entry,
)
from tasks.common import (
    FORWARDER_CONTAINER_APP_PREFIX,
    FORWARDER_METRIC_PREFIX,
    FORWARDER_STORAGE_ACCOUNT_PREFIX,
    Resource,
    get_container_app_name,
    get_managed_env_id,
    get_managed_env_name,
    get_storage_account_name,
    is_azure_gov,
    log_errors,
)
from tasks.concurrency import collect, create_task_from_awaitable
from tasks.constants import ALLOWED_CONTAINER_APP_REGIONS
from tasks.deploy_common import wait_for_resource

FORWARDER_METRIC_CONTAINER_NAME = "dd-forwarder"


FORWARDER_TIMEOUT_SECONDS = 1800  # 30 minutes
CLIENT_MAX_SECONDS = 5
MAX_ATTEMPS = 5

FORWARDER_METRIC_BLOB_LIFETIME_DAYS = 1


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


def get_metric_value(
    metric_entry: MetricBlobEntry,
    metric_name: Literal["resource_log_volume", "resource_log_bytes", "runtime_seconds"],
) -> float:
    value = metric_entry[metric_name]
    if isinstance(value, dict):
        return sum(value.values())
    return value


class LogForwarderClient(AbstractAsyncContextManager["LogForwarderClient"]):
    def __init__(
        self,
        log: Logger,
        credential: DefaultAzureCredential,
        subscription_id: str,
        resource_group: str,
        pii_rules_json: str,
        metrics_client: MetricsApi,
    ) -> None:
        self.forwarder_image = get_config_option(FORWARDER_IMAGE_SETTING)
        self.dd_api_key = get_config_option(DD_API_KEY_SETTING)
        self.dd_site = get_config_option(DD_SITE_SETTING)
        self.telemetry_enabled = is_truthy(DD_TELEMETRY_SETTING)
        self.control_plane_region = get_config_option(CONTROL_PLANE_REGION_SETTING)
        self.control_plane_id = get_config_option(CONTROL_PLANE_ID_SETTING)
        self.log = log
        self.resource_group = resource_group
        self.subscription_id = subscription_id
        self.pii_rules_json = pii_rules_json
        self.container_apps_client = ContainerAppsAPIClient(credential, subscription_id)
        self.storage_client = StorageManagementClient(credential, subscription_id)
        self.metrics_client = metrics_client
        self._blob_forwarder_data_lock = Lock()
        self._blob_forwarder_data: bytes | None = None
        self._background_tasks: set[AsyncTask[Any]] = set()
        self.log_extra = {
            "subscription_id": self.subscription_id,
            "resource_group": self.resource_group,
        }

    async def __aenter__(self) -> Self:
        await gather(
            self.container_apps_client.__aenter__(),
            self.storage_client.__aenter__(),
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._background_tasks:
            await wait(self._background_tasks)
        await gather(
            self.container_apps_client.__aexit__(exc_type, exc_val, exc_tb),
            self.storage_client.__aexit__(exc_type, exc_val, exc_tb),
        )

    def submit_background_task(self, coro: Coroutine[Any, Any, Any]) -> None:
        def _done_callback(task: AsyncTask[Any]) -> None:
            self._background_tasks.discard(task)
            if e := task.exception():
                self.log.error(
                    "Background task failed with an exception",
                    exc_info=e,
                    extra=self.log_extra,
                )

        task = create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(_done_callback)

    def get_container_app_region(self, region: str) -> str:
        if region in ALLOWED_CONTAINER_APP_REGIONS:
            return region
        return self.control_plane_region

    async def create_log_forwarder(self, region: str, config_id: str) -> LogForwarderType:
        storage_account_name = get_storage_account_name(config_id)

        await wait_for_resource(*await self.create_log_forwarder_storage_account(region, storage_account_name))

        maybe_errors = await gather(
            wait_for_resource(*await self.create_or_update_log_forwarder_container_app(region, config_id)),
            self.create_log_forwarder_containers(storage_account_name),
            self.create_log_forwarder_storage_management_policy(storage_account_name),
            return_exceptions=True,
        )
        log_errors(
            self.log,
            "Failed to create function app and/or get blob forwarder data",
            *maybe_errors,
            reraise=True,
            extra=self.log_extra,
        )

        # for now this is the only type we support
        return STORAGE_ACCOUNT_TYPE

    async def get_forwarder_resources(self, config_id: str) -> tuple[Job | None, StorageAccount | None]:
        # spawn them off at the same time
        get_job = create_task_from_awaitable(
            self.container_apps_client.jobs.get(self.resource_group, get_container_app_name(config_id))
        )
        get_secrets = create_task_from_awaitable(
            self.container_apps_client.jobs.list_secrets(self.resource_group, get_container_app_name(config_id))
        )
        get_storage_account = create_task_from_awaitable(
            self.storage_client.storage_accounts.get_properties(
                self.resource_group, get_storage_account_name(config_id)
            )
        )

        job = None
        with suppress(ResourceNotFoundError):
            job = await get_job
            # populate the configuration separately
            if not job.configuration:
                job.configuration = JobConfiguration(replica_timeout=FORWARDER_TIMEOUT_SECONDS)
            job.configuration.secrets = (await get_secrets).value
        storage_account = None
        with suppress(ResourceNotFoundError):
            storage_account = await get_storage_account
        return job, storage_account

    async def create_log_forwarder_storage_account(
        self, region: str, storage_account_name: str
    ) -> ResourcePoller[StorageAccount]:
        self.log.info(
            "Creating storage account %s for region %s",
            storage_account_name,
            region,
            extra=self.log_extra,
        )
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
                minimum_tls_version="TLS1_2",
            ),
        ), lambda: self.storage_client.storage_accounts.get_properties(self.resource_group, storage_account_name)

    async def create_log_forwarder_managed_environment(self, region: str, wait: bool = False) -> None:
        container_app_region = self.get_container_app_region(region)
        env_name = get_managed_env_name(container_app_region, self.control_plane_id)
        self.log.info(
            "Creating managed environment %s for region %s in %s",
            env_name,
            region,
            container_app_region,
            extra=self.log_extra,
        )
        poller = await self.container_apps_client.managed_environments.begin_create_or_update(
            self.resource_group,
            env_name,
            ManagedEnvironment(
                location=container_app_region,
                zone_redundant=False,
            ),
        )
        if wait:
            await poller.result()

    async def get_log_forwarder_managed_environment(self, region: str) -> str | None:
        env_name = get_managed_env_name(self.get_container_app_region(region), self.control_plane_id)
        try:
            managed_env = await self.container_apps_client.managed_environments.get(self.resource_group, env_name)
        except ResourceNotFoundError:
            return None
        return str(managed_env.id)

    async def create_or_update_log_forwarder_container_app(
        self,
        region: str,
        config_id: str,
        *,
        env: list[EnvironmentVar] | None = None,
        secrets: list[Secret] | None = None,
    ) -> ResourcePoller[Job]:
        job_name = get_container_app_name(config_id)
        forwarder_region = self.get_container_app_region(region)
        env = env or self.generate_forwarder_settings(config_id)
        secrets = secrets or await self.generate_forwarder_secrets(config_id, region)
        return await self.container_apps_client.jobs.begin_create_or_update(
            self.resource_group,
            job_name,
            Job(
                location=forwarder_region,
                environment_id=get_managed_env_id(
                    self.subscription_id,
                    self.resource_group,
                    forwarder_region,
                    self.control_plane_id,
                ),
                configuration=JobConfiguration(
                    trigger_type="Schedule",
                    schedule_trigger_config=JobConfigurationScheduleTriggerConfig(
                        cron_expression="* * * * *",
                        parallelism=1,
                        replica_completion_count=1,
                    ),
                    replica_timeout=FORWARDER_TIMEOUT_SECONDS,
                    replica_retry_limit=1,
                    secrets=secrets,
                ),
                template=JobTemplate(
                    containers=[
                        Container(
                            name="forwarder",
                            image=self.forwarder_image,
                            resources=ContainerResources(cpu=2, memory="4Gi"),
                            env=env,
                        )
                    ],
                ),
            ),
        ), lambda: self.container_apps_client.jobs.get(self.resource_group, job_name)

    async def generate_forwarder_secrets(self, config_id: str, storage_region: str) -> list[Secret]:
        connection_string = await self.get_connection_string(get_storage_account_name(config_id), storage_region)
        return [
            Secret(name=DD_API_KEY_SECRET, value=self.dd_api_key),
            Secret(name=CONNECTION_STRING_SECRET, value=connection_string),
        ]

    def generate_forwarder_settings(self, config_id: str) -> list[EnvironmentVar]:
        return [
            EnvironmentVar(name=STORAGE_CONNECTION_SETTING, secret_ref=CONNECTION_STRING_SECRET),
            EnvironmentVar(name=DD_API_KEY_SETTING, secret_ref=DD_API_KEY_SECRET),
            EnvironmentVar(name=DD_SITE_SETTING, value=self.dd_site),
            EnvironmentVar(name=CONTROL_PLANE_ID_SETTING, value=self.control_plane_id),
            EnvironmentVar(name=CONFIG_ID_SETTING, value=config_id),
            EnvironmentVar(name=PII_SCRUBBER_RULES_SETTING, value=self.pii_rules_json),
            EnvironmentVar(name=DD_TELEMETRY_SETTING, value=str(self.telemetry_enabled).lower()),
        ]

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
                                ),
                            ),
                        )
                    ]
                )
            ),
        )

    async def get_connection_string(self, storage_account_name: str, storage_account_region: str) -> str:
        keys_result = await self.storage_client.storage_accounts.list_keys(self.resource_group, storage_account_name)
        keys: list[StorageAccountKey] = keys_result.keys  # type: ignore
        if len(keys) == 0:
            raise ValueError("No keys found for storage account")
        key: str = keys[0].value  # type: ignore
        endpoint_suffix = "core.{}.net".format("usgovcloudapi" if is_azure_gov(storage_account_region) else "windows")
        return f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={key};EndpointSuffix={endpoint_suffix}"

    async def delete_log_forwarder(self, forwarder_id: str, *, raise_error: bool = True, max_attempts: int = 3) -> bool:
        """Deletes the Log forwarder, returns True if successful, False otherwise"""

        @retry(stop=stop_after_attempt(max_attempts), retry=is_exception_retryable)
        async def _delete_forwarder() -> None:
            self.log.info(
                "Attempting to delete log forwarder %s",
                forwarder_id,
                extra=self.log_extra,
            )

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

                await delete_storage_account_task
            self.log.info("Deleted log forwarder %s", forwarder_id, extra=self.log_extra)

        try:
            await _delete_forwarder()
            return True
        except Exception:
            if raise_error:
                raise
            return False

    async def delete_log_forwarder_env(self, region: str, *, raise_error: bool = True, max_attempts: int = 3) -> bool:
        """Deletes the Log forwarder env, returns True if successful, False otherwise"""

        @retry(stop=stop_after_attempt(max_attempts), retry=is_exception_retryable)
        async def _delete_forwarder_env() -> None:
            self.log.info(
                "Attempting to delete log forwarder env for region %s and control plane %s",
                region,
                self.control_plane_id,
                extra=self.log_extra,
            )

            poller = await ignore_exception_type(
                ResourceNotFoundError,
                self.container_apps_client.managed_environments.begin_delete(
                    self.resource_group,
                    get_managed_env_name(region, self.control_plane_id),
                ),
            )
            if poller:
                await poller.result()

            self.log.info(
                "Deleted log forwarder env for region %s and control plane %s",
                region,
                self.control_plane_id,
                extra=self.log_extra,
            )

        try:
            await _delete_forwarder_env()
            return True
        except Exception:
            if raise_error:
                raise
            return False

    async def collect_forwarder_metrics(
        self, config_id: str, region: str, oldest_valid_timestamp: float
    ) -> list[MetricBlobEntry]:
        """Collects metrics for a given forwarder and submits them to the metrics endpoint"""
        metric_lines = await self.get_blob_metrics_lines(config_id, region)
        forwarder_metrics = [
            metric_entry
            for metric_line in metric_lines
            if (metric_entry := deserialize_blob_metric_entry(metric_line, oldest_valid_timestamp))
        ]
        if not forwarder_metrics:
            self.log.warning(
                "No valid metrics found for forwarder %s",
                config_id,
                extra=self.log_extra,
            )
        self.submit_background_task(self.submit_log_forwarder_metrics(config_id, forwarder_metrics, region))
        return forwarder_metrics

    async def get_blob_metrics_lines(self, config_id: str, region) -> list[str]:
        """
        Returns a list of json decodable strings that represent metrics
        json string takes form of {'Values': [metric_dict]}
        metric_dict is as follows {'Name': str, 'Value': float, 'Time': float}
        Time is a unix timestamp
        """
        conn_str = await self.get_connection_string(get_storage_account_name(config_id), region)
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
                    self.log.error(
                        "Unable to fetch metrics in %s for forwarder %s:\n%s",
                        blob,
                        config_id,
                        msg,
                        extra=self.log_extra,
                    )

            return metric_lines

    @retry(retry=is_exception_retryable, stop=stop_after_attempt(MAX_ATTEMPS))
    async def read_blob(self, container_client: ContainerClient, blob_name: str) -> str:
        try:
            async with container_client.get_blob_client(blob_name) as blob_client:
                raw_data: StorageStreamDownloader[bytes] = await blob_client.download_blob(timeout=CLIENT_MAX_SECONDS)
                dict_str = await raw_data.readall()
                return dict_str.decode("utf-8")
        except ResourceNotFoundError:
            return ""

    @retry(retry=is_exception_retryable, stop=stop_after_attempt(MAX_ATTEMPS))
    async def submit_log_forwarder_metrics(
        self, log_forwarder_id: str, metrics: list[MetricBlobEntry], region: str
    ) -> None:
        if not self.telemetry_enabled or not metrics:
            return

        response: IntakePayloadAccepted = await self.metrics_client.submit_metrics(
            body=self.create_metric_payload(metrics, log_forwarder_id, region)
        )  # type: ignore
        for error in response.get("errors", []):
            self.log.error(error, extra=self.log_extra)

    def create_metric_payload(
        self, metric_entries: list[MetricBlobEntry], log_forwarder_id: str, region: str
    ) -> MetricPayload:
        log_forwarder_name = get_container_app_name(log_forwarder_id)
        version = metric_entries[0].get("version", "unknown") if metric_entries else "unknown"
        tags = [
            f"control_plane_id:{self.control_plane_id}",
            f"region:{region}",
            f"version:{version}",
        ]
        metric_series = [
            MetricSeries(
                metric=FORWARDER_METRIC_PREFIX + metric_name,
                type=MetricIntakeType.UNSPECIFIED,
                points=[
                    MetricPoint(
                        timestamp=int(metric_entry["timestamp"]),
                        value=get_metric_value(metric_entry, metric_name),
                    )
                    for metric_entry in metric_entries
                ],
                resources=[
                    MetricResource(
                        name=log_forwarder_name,
                        type="logforwarder",
                    ),
                ],
                tags=tags,
            )
            for metric_name in METRIC_NAMES
        ]
        metric_series.append(
            MetricSeries(
                metric=FORWARDER_METRIC_PREFIX + "run_completed",
                type=MetricIntakeType.UNSPECIFIED,
                points=[
                    MetricPoint(
                        timestamp=int(metric_entry["timestamp"]),
                        value=1,
                    )
                    for metric_entry in metric_entries
                ],
                resources=[
                    MetricResource(
                        name=log_forwarder_name,
                        type="logforwarder",
                    ),
                ],
                tags=tags,
            )
        )
        return MetricPayload(series=metric_series)

    async def list_log_forwarder_ids(self) -> set[str]:
        jobs, storage_accounts = await gather(
            collect(self.container_apps_client.jobs.list_by_resource_group(self.resource_group)),
            collect(self.storage_client.storage_accounts.list_by_resource_group(self.resource_group)),
        )

        def _get_forwarder_config_ids(it: Iterable[Resource], prefix: str) -> set[str]:
            return {resource.name.removeprefix(prefix) for resource in it if resource.name.startswith(prefix)}

        return _get_forwarder_config_ids(
            cast(Iterable[Resource], jobs), FORWARDER_CONTAINER_APP_PREFIX
        ) | _get_forwarder_config_ids(cast(Iterable[Resource], storage_accounts), FORWARDER_STORAGE_ACCOUNT_PREFIX)
