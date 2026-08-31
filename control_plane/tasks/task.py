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
from datadog.dogstatsd.base import statsd
from datadog_api_client import AsyncApiClient, Configuration
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.http_log import HTTPLog
from datadog_api_client.v2.model.http_log_item import HTTPLogItem

# project
from cache.common import read_cache
from cache.env import (
    CONTROL_PLANE_ID_SETTING,
    DD_API_KEY_SETTING,
    DD_SITE_SETTING,
    LOG_LEVEL_SETTING,
)
from tasks.client.datadog_api_client import DatadogClient, StatusCode
from tasks.common import (
    CONTROL_PLANE_METRIC_PREFIX,
    CONTROL_PLANE_METRIC_TAG,
    TASK_RUN_COMPLETED_METRIC,
    TASK_STARTED_METRIC,
    create_credential,
    now,
)
from tasks.telemetry import TELEMETRY_ENABLED
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
        self.credential = create_credential()

        # Telemetry Logic
        self.start_time = time()
        self.execution_id = execution_id if execution_id else str(uuid4())
        self.control_plane_id = environ.get(CONTROL_PLANE_ID_SETTING, "unknown")
        self.tags = [
            CONTROL_PLANE_METRIC_TAG,
            f"task:{self.NAME}",
            f"control_plane_id:{self.control_plane_id}",
            f"version:{VERSION}",
        ]
        self.log = log.getChild(self.__class__.__name__)
        self._logs: list[LogRecord] = []
        configuration = Configuration()

        target_staging = TELEMETRY_ENABLED and environ.get(DD_SITE_SETTING) == "datad0g.com"

        if target_staging:
            configuration.server_index = 2
            configuration.server_variables["site"] = "datad0g.com"

            host_settings = configuration.get_host_settings()
            _add_datadog_staging(host_settings)
            configuration.get_host_settings = lambda: host_settings

        self._datadog_client = AsyncApiClient(configuration)
        self._logs_client = LogsApi(self._datadog_client)

        self._datadog_api_client = DatadogClient(environ.get(DD_SITE_SETTING), environ.get(DD_API_KEY_SETTING))
        if target_staging:
            logs_servers = self._logs_client._submit_log_endpoint.settings.get("servers")
            _add_datadog_staging(logs_servers)

        if TELEMETRY_ENABLED:
            log.info("Telemetry enabled, will submit logs for %s", self.NAME)
            self.log.addHandler(ListHandler(self._logs))

        self._is_initial_run = is_initial_run

    @abstractmethod
    async def run(self) -> None: ...

    def submit_control_plane_metric(self, metric_name: str, value: float) -> None:
        """Emits a control plane metric immediately, rather than at teardown like the telemetry
        gauges. A run that is killed mid-flight never reaches teardown, so anything emitted there
        is exactly the signal that goes missing when it is most needed."""
        if not TELEMETRY_ENABLED:
            return
        statsd.count(CONTROL_PLANE_METRIC_PREFIX + metric_name, value, tags=self.tags)

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
                self.submit_control_plane_metric(TASK_RUN_COMPLETED_METRIC, 1)
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
        if not TELEMETRY_ENABLED:
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
        statsd.gauge_with_timestamp(
            CONTROL_PLANE_METRIC_PREFIX + "task_completed", 1, int(self.start_time), tags=self.tags
        )
        statsd.gauge_with_timestamp(
            CONTROL_PLANE_METRIC_PREFIX + "runtime_seconds",
            time() - self.start_time,
            int(self.start_time),
            tags=self.tags,
        )
        if self._logs:
            self._logs.clear()
            await self._logs_client.submit_log(dd_logs, ddtags=",".join(self.tags))  # type: ignore

    async def submit_status_update(self, step: str, status: StatusCode, message: str) -> None:
        """Submits a status update to the LFO status endpoint.

        Error statuses are always submitted. Non-error statuses are only submitted on the initial
        run, since the endpoint backs the onboarding workflow UI and steady-state progress updates
        would be noise. Errors are the signal an operator needs on every run, and suppressing them
        outside onboarding is why a scaling task stuck in a create/delete loop stayed invisible.
        """
        if not self._is_initial_run and status is StatusCode.OK:
            return
        # never let status reporting abort the caller: these are awaited from inside except
        # handlers that still have cleanup to do, and an unreachable status endpoint must not
        # become the reason a storage account is leaked
        try:
            await self._datadog_api_client.submit_status_update(
                f"{self.NAME}.{step}", status, message, self.execution_id, VERSION, self.control_plane_id
            )
        except Exception:
            # self.log, not the module logger: the ListHandler that feeds Datadog log submission
            # is attached to the child logger, and records propagate up but never down
            self.log.exception("Failed to submit status update for %s.%s", self.NAME, step)


async def task_main(task_class: type[Task], caches: list[str], is_initial_run: bool = False) -> None:
    level = environ.get(LOG_LEVEL_SETTING, "INFO").upper()
    if level not in {"ERROR", "WARN", "WARNING", "INFO", "DEBUG"}:
        level = "INFO"
    basicConfig()
    log.setLevel(level)
    log.info("Started %s at %s (log level %s)", task_class.NAME, now(), level)
    cache_states = await gather(*map(read_cache, caches))
    task = task_class(*cache_states, is_initial_run=is_initial_run)
    # emitted before __aenter__ so a run killed during client setup still shows up in the
    # started - completed delta; anything emitted inside would be missed by exactly those deaths
    task.submit_control_plane_metric(TASK_STARTED_METRIC, 1)
    async with task:
        await task.run()
    log.info("%s finished at %s", task_class.NAME, now())
