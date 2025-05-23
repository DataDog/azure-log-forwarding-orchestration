# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from abc import abstractmethod
from asyncio import create_task, gather
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from logging import ERROR, Handler, LogRecord, basicConfig, getLogger
from os import environ
from time import time
from traceback import format_exception
from types import TracebackType
from typing import Any, Self
from uuid import uuid4

# 3p
from azure.identity.aio import DefaultAzureCredential
from datadog_api_client import AsyncApiClient, Configuration
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.api.metrics_api import MetricsApi
from datadog_api_client.v2.model.http_log import HTTPLog
from datadog_api_client.v2.model.http_log_item import HTTPLogItem
from datadog_api_client.v2.model.metric_payload import MetricPayload
from datadog_api_client.v2.model.metric_point import MetricPoint
from datadog_api_client.v2.model.metric_series import MetricSeries

# project
from cache.common import read_cache
from cache.env import (
    CONTROL_PLANE_ID_SETTING,
    DD_API_KEY_SETTING,
    DD_SITE_SETTING,
    DD_TELEMETRY_SETTING,
    LOG_LEVEL_SETTING,
    is_truthy,
)
from tasks.client.datadog_api_client import DatadogClient, StatusCode
from tasks.common import CONTROL_PLANE_METRIC_PREFIX, now
from tasks.version import VERSION

log = getLogger(__name__)

# silence azure logging except for errors
getLogger("azure").setLevel(ERROR)

IGNORED_LOG_EXTRAS = {"created", "relativeCreated", "thread", "args", "msg", "message"}


def get_error_telemetry(
    exc_info: tuple[type[BaseException], BaseException, TracebackType | None] | tuple[None, None, None] | None,
) -> dict[str, str]:
    telemetry = {}
    if not exc_info:
        return telemetry
    exc_type, exc, tb = exc_info
    if exc_type:
        telemetry["exception"] = exc_type.__name__
    if exc_type or exc or tb:
        telemetry["exc_info"] = "".join(format_exception(exc_type, value=exc, tb=tb, limit=20))
    return telemetry


class ListHandler(Handler):
    """A logging handler that appends log messages to a list"""

    def __init__(self, logs: list[LogRecord]):
        super().__init__()
        self.log_list = logs

    def emit(self, record: LogRecord) -> None:
        record.asctime = datetime.now(UTC).isoformat()
        self.log_list.append(record)


def _add_datadog_staging(settings: list[dict[str, Any]] | None) -> None:
    """takes a list of settings and adds datad0g.com to the list of supported sites"""
    if not settings or not isinstance(settings, list):
        return
    supported_sites = settings[0].get("variables", {}).get("site", {}).get("enum_values", [])
    if len(supported_sites) > 1:
        supported_sites.append("datad0g.com")


class Task(AbstractAsyncContextManager["Task"]):
    NAME: str

    def __init__(self, execution_id: str | None = "", is_initial_run: bool = False) -> None:
        self.credential = DefaultAzureCredential()

        # Telemetry Logic
        self.start_time = time()
        self.execution_id = execution_id if execution_id else str(uuid4())
        self.control_plane_id = environ.get(CONTROL_PLANE_ID_SETTING, "unknown")
        self.tags = [
            "forwarder:lfocontrolplane",
            f"task:{self.NAME}",
            f"control_plane_id:{self.control_plane_id}",
            f"version:{VERSION}",
        ]
        self.telemetry_enabled = bool(is_truthy(DD_TELEMETRY_SETTING) and environ.get(DD_API_KEY_SETTING))
        self.log = log.getChild(self.__class__.__name__)
        self._logs: list[LogRecord] = []
        configuration = Configuration()

        target_staging = self.telemetry_enabled and "datad0g.com" in environ.get(DD_SITE_SETTING, "")

        if target_staging:
            configuration.server_index = 2
            configuration.server_variables["site"] = "datad0g.com"

            host_settings = configuration.get_host_settings()
            _add_datadog_staging(host_settings)
            configuration.get_host_settings = lambda: host_settings

        self._datadog_client = AsyncApiClient(configuration)
        self._logs_client = LogsApi(self._datadog_client)
        self._metrics_client = MetricsApi(self._datadog_client)

        self._datadog_api_client = DatadogClient(environ.get(DD_SITE_SETTING), environ.get(DD_API_KEY_SETTING))
        if target_staging:
            logs_servers = self._logs_client._submit_log_endpoint.settings.get("servers")
            _add_datadog_staging(logs_servers)

            metrics_servers = self._metrics_client._submit_metrics_endpoint.settings.get("servers")
            _add_datadog_staging(metrics_servers)

        if self.telemetry_enabled:
            log.info("Telemetry enabled, will submit logs for %s", self.NAME)
            self.log.addHandler(ListHandler(self._logs))

        self._is_initial_run = is_initial_run

    @abstractmethod
    async def run(self) -> None: ...

    async def __aenter__(self) -> Self:
        await gather(
            self.credential.__aenter__(), self._datadog_client.__aenter__(), self._datadog_api_client.__aenter__()
        )
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        try:
            submit_telemetry = create_task(self.submit_telemetry())
            if exc_type is None and exc_value is None and traceback is None:
                await self.write_caches()
            try:
                await submit_telemetry
            except Exception:
                log.exception("Failed to submit telemetry")
        finally:
            await self.credential.__aexit__(exc_type, exc_value, traceback)
            await self._datadog_client.__aexit__(exc_type, exc_value, traceback)
            await self._datadog_api_client.__aexit__(exc_type, exc_value, traceback)

    @abstractmethod
    async def write_caches(self) -> None: ...

    async def submit_telemetry(self) -> None:
        if not self.telemetry_enabled:
            return
        dd_logs = HTTPLog(
            value=[
                HTTPLogItem(
                    **{
                        **{k: str(v) for k, v in record.__dict__.items() if k.lower() not in IGNORED_LOG_EXTRAS},
                        **{
                            "message": record.getMessage(),
                            "ddsource": "azure",
                            "service": "lfo",
                            "time": record.asctime,
                            "level": record.levelname,
                            "execution_id": self.execution_id,
                            "control_plane_id": self.control_plane_id,
                            "task": self.NAME,
                        },
                        **get_error_telemetry(record.exc_info),
                    }
                )
                for record in self._logs
            ]
        )
        runtime_seconds = MetricSeries(
            metric=CONTROL_PLANE_METRIC_PREFIX + "runtime_seconds",
            points=[MetricPoint(timestamp=int(self.start_time), value=time() - self.start_time)],
            tags=self.tags,
        )
        task_completed = MetricSeries(
            metric=CONTROL_PLANE_METRIC_PREFIX + "task_completed",
            points=[MetricPoint(timestamp=int(self.start_time), value=1)],
            tags=self.tags,
        )
        await self._metrics_client.submit_metrics(MetricPayload(series=[runtime_seconds, task_completed]))  # type: ignore
        if self._logs:
            self._logs.clear()
            await self._logs_client.submit_log(dd_logs, ddtags=",".join(self.tags))  # type: ignore

    async def submit_status_update(self, step: str, status: StatusCode, message: str) -> None:
        if not self._is_initial_run:
            return
        await self._datadog_api_client.submit_status_update(
            f"{self.NAME}.{step}", status, message, self.execution_id, VERSION, self.control_plane_id
        )


async def task_main(task_class: type[Task], caches: list[str], is_initial_run: bool = False) -> None:
    level = environ.get(LOG_LEVEL_SETTING, "INFO").upper()
    if level not in {"ERROR", "WARN", "WARNING", "INFO", "DEBUG"}:
        level = "INFO"
    basicConfig()
    log.setLevel(level)
    log.info("Started %s at %s (log level %s)", task_class.NAME, now(), level)
    cache_states = await gather(*map(read_cache, caches))
    async with task_class(*cache_states, is_initial_run=is_initial_run) as task:
        await task.run()
    log.info("%s finished at %s", task_class.NAME, now())
