# stdlib
import os
from asyncio import Lock, create_task, gather, run, wait
from asyncio import Task as AsyncTask
from collections.abc import Awaitable, Coroutine
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from json import JSONDecodeError, dumps, loads
from logging import DEBUG, INFO, basicConfig, getLogger
from types import TracebackType
from typing import Any, AsyncContextManager, Self, TypeAlias, TypeVar
from uuid import uuid4

# 3p
from aiohttp import ClientSession
from aiosonic.exceptions import RequestTimeout  # type: ignore
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
from jsonschema import ValidationError, validate
from tenacity import RetryCallState, RetryError, retry, stop_after_attempt

# project
from cache.assignment_cache import ASSIGNMENT_CACHE_BLOB, deserialize_assignment_cache
from cache.common import (
    STORAGE_ACCOUNT_TYPE,
    DiagnosticSettingType,
    InvalidCacheError,
    get_app_service_plan_name,
    get_config_option,
    get_function_app_id,
    get_function_app_name,
    get_storage_account_name,
    read_cache,
    write_cache,
)
from cache.resources_cache import RESOURCE_CACHE_BLOB, deserialize_resource_cache
from tasks.task import Task, now, wait_for_resource

SCALING_TASK_NAME = "scaling_task"

BLOB_FORWARDER_DATA_CONTAINER, BLOB_FORWARDER_DATA_BLOB = "blob-forwarder", "data.zip"

COLLECTED_METRIC_DEFINITIONS = {"FunctionExecutionCount": "total"}

METRIC_COLLECTION_PERIOD_MINUTES = 30  # How long we are mointoring in minutes
METRIC_COLLECTION_SAMPLES = 6  # Number of samples we are collecting
METRIC_COLLECTION_GRANULARITY = METRIC_COLLECTION_PERIOD_MINUTES // METRIC_COLLECTION_SAMPLES

CLIENT_MAX_SECONDS_PER_METRIC = 5
CLIENT_MAX_SECONDS = CLIENT_MAX_SECONDS_PER_METRIC * len(COLLECTED_METRIC_DEFINITIONS)
MAX_ATTEMPS = 5

SHOULD_SUBMIT_METRICS = os.environ.get("SHOULD_SUBMIT_METRICS", False)

METRIC_BLOB_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "Values": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "Name": {"type": "string"},
                    "Value": {"type": "number"},
                    "Time": {"type": "number"},
                },
                "additionalProperties": False,
            },
        }
    },
    "additionalProperties": False,
}


log = getLogger(SCALING_TASK_NAME)
log.setLevel(DEBUG)

LogForwarderMetrics: TypeAlias = dict[str, dict[str, float]]
"""
Type alias that represents the result of collecting the log forwarder metrics.
It is a mapping of log forwarder/config ids to metric names to the max metric value over the timeseries.
"""

LogForwarderBlobMetrics: TypeAlias = dict[str, list[dict[str, float | str]]]


async def is_exception_retryable(state: RetryCallState) -> bool:
    if (future := state.outcome) and (e := future.exception()):
        if isinstance(e, HttpResponseError):
            return e.status_code is not None and (e.status_code == 429 or e.status_code >= 500)
        if isinstance(e, RequestTimeout) or isinstance(e, ServiceResponseTimeoutError):
            return True
    return False


T = TypeVar("T")


async def ignore_exception_type(exc: type[BaseException], a: Awaitable[T]) -> T | None:
    try:
        return await a
    except exc:
        return None


class LogForwarderClient(AsyncContextManager):
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

    async def create_log_forwarder(self, region: str, config_id: str) -> DiagnosticSettingType:
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
        key = keys[0].value
        return f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={key};EndpointSuffix=core.windows.net"

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

    async def get_blob_metrics(self, connection_str: str, container_name: str) -> list[str]:
        """
        Returns a list of json decodable strings that represent metrics
        json string takes form of {'Values': [metric_dict]}
        metric_dict is as follows {'Name': str, 'Value': float, 'Time': float}
        Time is a unix timestamp
        """
        async with ContainerClient.from_connection_string(connection_str, container_name) as container_client:
            metrics = []
            current_time: datetime = datetime.now(UTC)
            previous_hour: datetime = current_time - timedelta(hours=1)
            results = await gather(
                *[
                    self.read_blob(container_client, self.get_datetime_str(previous_hour)),
                    self.read_blob(container_client, self.get_datetime_str(current_time)),
                ]
            )
            for result in results:
                metrics.extend(result)
            return metrics

    @retry(retry=is_exception_retryable, stop=stop_after_attempt(MAX_ATTEMPS))
    async def read_blob(self, container_client: ContainerClient, blob_name: str) -> list[str]:
        try:
            async with container_client.get_blob_client(blob_name) as blob_client:
                raw_data = await blob_client.download_blob()
                dict_str = await raw_data.readall()
                return dict_str.decode("utf-8").split("\n")
        except ResourceNotFoundError:
            return []

    @retry(retry=is_exception_retryable, stop=stop_after_attempt(MAX_ATTEMPS))
    async def submit_log_forwarder_metrics(self, log_forwarder_id: str, metrics: list[Metric], sub_id: str) -> None:
        if "DD_API_KEY" not in os.environ:
            return
        metric_series: list[MetricSeries] = [self.create_metric_series(metric, log_forwarder_id) for metric in metrics]  # type: ignore
        if metric_series is None or not all(metric_series):
            log.warn(
                f"Invalid timestamps for resource: {get_function_app_id(sub_id, self.resource_group, log_forwarder_id)}\nSkipping..."
            )
            return
        body = MetricPayload(
            series=metric_series,
        )

        response = await self.api_instance.submit_metrics(body=body)  # type: ignore
        if len(response.get("errors", [])) > 0:
            for err in response.get("errors", []):
                log.error(err)

    def get_datetime_str(self, time: datetime) -> str:
        log.info(f"{time:%Y-%m-%d-%H}")
        return f"{time:%Y-%m-%d-%H}"

    def create_metric_series(self, metric: Metric, log_forwarder_id: str) -> MetricSeries | None:
        metric_points: list[MetricPoint | None] = [
            self.create_metric_point(metric_value, COLLECTED_METRIC_DEFINITIONS.get(metric.name))  # type: ignore
            for time_series_element in metric.timeseries
            for metric_value in time_series_element.data
        ]
        filtered_metric_points = [metric_point for metric_point in metric_points if metric_point is not None]
        if len(metric_points) == 0:
            return None
        return MetricSeries(
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
        return MetricPoint(
            timestamp=int(metric_timestamp),
            value=getattr(metric_value, metric_attr),
        )


class ScalingTask(Task):
    def __init__(self, resource_cache_state: str, assignment_cache_state: str, resource_group: str) -> None:
        super().__init__()
        self.resource_group = resource_group

        self.background_tasks: set[AsyncTask[Any]] = set()

        # Resource Cache
        success, resource_cache = deserialize_resource_cache(resource_cache_state)
        if not success:
            raise InvalidCacheError("Resource Cache is in an invalid format, failing this task until it is valid")
        self.resource_cache = resource_cache

        # Assignment Cache
        success, assignment_cache = deserialize_assignment_cache(assignment_cache_state)
        if not success:
            log.warning("Assignment Cache is in an invalid format, task will reset the cache")
            assignment_cache = {}
        self._assignment_cache_initial_state = assignment_cache
        self.assignment_cache = deepcopy(assignment_cache)

    async def run(self) -> None:
        log.info("Running for %s subscriptions: %s", len(self.resource_cache), list(self.resource_cache.keys()))
        all_subscriptions = set(self.resource_cache.keys()) | set(self.assignment_cache.keys())
        await gather(*(self.process_subscription(sub_id) for sub_id in all_subscriptions))

    async def process_subscription(self, subscription_id: str) -> None:
        previous_region_assignments = set(self._assignment_cache_initial_state.get(subscription_id, {}).keys())
        current_regions = set(self.resource_cache.get(subscription_id, {}).keys())
        regions_to_add = current_regions - previous_region_assignments
        regions_to_remove = previous_region_assignments - current_regions
        regions_to_check_scaling = current_regions & previous_region_assignments
        async with LogForwarderClient(self.credential, subscription_id, self.resource_group) as client:
            tasks: list[Coroutine[Any, Any, None]] = []
            tasks.extend(self.create_log_forwarder(client, subscription_id, region) for region in regions_to_add)
            tasks.extend(
                self.delete_region_log_forwarders(client, subscription_id, region) for region in regions_to_remove
            )
            tasks.extend(
                self.check_region_scaling(client, subscription_id, region) for region in regions_to_check_scaling
            )

            await gather(*tasks)
            if len(self.background_tasks) != 0:
                await wait(self.background_tasks)

        self.update_assignments(subscription_id)

    async def create_log_forwarder(
        self,
        client: LogForwarderClient,
        subscription_id: str,
        region: str,
    ) -> None:
        log.info("Creating log forwarder for subscription %s in region %s", subscription_id, region)
        config_id = str(uuid4())[-12:]  # take the last section since we are length limited
        try:
            config_type = await client.create_log_forwarder(region, config_id)
        except Exception:
            log.exception("Failed to create log forwarder %s, cleaning up", config_id)
            success = await client.delete_log_forwarder(config_id, raise_error=False)
            if not success:
                log.error("Failed to clean up log forwarder %s, manual intervention required", config_id)
            return
        self.assignment_cache.setdefault(subscription_id, {})[region] = {
            "configurations": {config_id: config_type},
            "resources": {},
        }

    async def delete_region_log_forwarders(
        self,
        client: LogForwarderClient,
        subscription_id: str,
        region: str,
    ) -> None:
        log.info("Deleting log forwarder for subscription %s in region %s", subscription_id, region)
        await gather(
            *(
                client.delete_log_forwarder(forwarder_id)
                for forwarder_id in self.assignment_cache[subscription_id][region]["configurations"]
            )
        )
        del self.assignment_cache[subscription_id][region]

    async def check_region_scaling(
        self,
        client: LogForwarderClient,
        subscription_id: str,
        region: str,
    ) -> None:
        log.info("Checking scaling for log forwarders in region %s", region)

        forwarder_metrics = await gather(
            *(
                self.collect_forwarder_metrics(config_id, subscription_id, client)
                for config_id, config_type in self.assignment_cache[subscription_id][region]["configurations"].items()
            )
        )

        await gather(*(self.background_tasks))

        # TODO: AZINTS-2388 implement logic to scale the forwarders based on the metrics

    async def collect_forwarder_metrics(
        self, config_id: str, sub_id: str, client: LogForwarderClient
    ) -> LogForwarderMetrics:
        """Updates the log_forwarder_metric_cache entry for a log forwarder
        If there is an error the entry is set to an empty dict"""
        metric_dict = {}
        # TODO Figure out how to get actual connection string + container name
        try:
            metric_dicts = await client.get_blob_metrics(
                get_config_option("TEST_CONNECTION_STR"), "insights-logs-functionapplogs"
            )
            oldest_time: datetime = datetime.now() - timedelta(minutes=30)
            forwarder_metrics = [
                metric_list
                for metric_list in [
                    self.validate_blob_metric_dict(mlist, oldest_time.timestamp()) for mlist in metric_dicts
                ]
                if metric_list is not None
            ]
            for met in forwarder_metrics:
                log.info(met)
            return metric_dict
        except HttpResponseError:
            log.exception("Recieved azure HTTP error: ")
            return {}
        except RetryError:
            log.error("Max retries attempted")
            return {}

    def validate_blob_metric_dict(self, blob_dict_str: str, oldest_legal_time: float) -> LogForwarderBlobMetrics | None:
        try:
            blob_dict: LogForwarderBlobMetrics = loads(blob_dict_str)
            validate(instance=blob_dict, schema=METRIC_BLOB_SCHEMA)
            if len(blob_dict["Values"]) == 0:
                return None
            # This is validated previously via the schema so this will always be legal
            if blob_dict["Values"][0]["Time"] < oldest_legal_time:  # type: ignore
                return None
            return blob_dict
        except (JSONDecodeError, ValidationError):
            return None

    def update_assignments(self, sub_id: str) -> None:
        for region, region_config in self.assignment_cache[sub_id].items():
            diagnostic_setting_configurations = region_config["configurations"]
            assert (
                len(diagnostic_setting_configurations) == 1
            )  # TODO(AZINTS-2388) right now we only have one config per region

            # just use the one config we have for now
            config_id = list(diagnostic_setting_configurations.keys())[0]

            resource_assignments = region_config["resources"]

            # update all the resource assignments to use the new config
            for resource_id in self.resource_cache[sub_id][region]:
                resource_assignments[resource_id] = config_id

    async def write_caches(self) -> None:
        if self.assignment_cache == self._assignment_cache_initial_state:
            log.info("Assignments have not changed, no update needed")
            return
        await write_cache(ASSIGNMENT_CACHE_BLOB, dumps(self.assignment_cache))
        log.info("Updated assignments stored in the cache")


async def main() -> None:
    basicConfig(level=INFO)
    log.info("Started task at %s", now())
    resource_group = get_config_option("RESOURCE_GROUP")
    # resources_cache_state, assignment_cache_state = await gather(
    #     read_cache(RESOURCE_CACHE_BLOB),
    #     read_cache(ASSIGNMENT_CACHE_BLOB),
    # )
    resources_cache_state = dumps({"0b62a232-b8db-4380-9da6-640f7272ed6d": {"eastus": ["resource1"]}})
    assignment_cache_state = dumps(
        {
            "0b62a232-b8db-4380-9da6-640f7272ed6d": {
                "eastus": {
                    "resources": {"resource1": "d76404b14764"},
                    "configurations": {
                        "d76404b14764": STORAGE_ACCOUNT_TYPE,
                    },
                }
            },
        }
    )

    async with ScalingTask(resources_cache_state, assignment_cache_state, resource_group) as task:
        await task.run()
    log.info("Task finished at %s", now())


if __name__ == "__main__":
    run(main())
