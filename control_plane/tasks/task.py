# stdlib
from abc import abstractmethod
from asyncio import create_task, gather
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from logging import ERROR, Handler, LogRecord, basicConfig, getLogger
from os import environ
from traceback import format_exception
from types import TracebackType
from typing import Self
from uuid import uuid4

# 3p
from azure.identity.aio import DefaultAzureCredential
from datadog_api_client import AsyncApiClient, Configuration
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.http_log import HTTPLog
from datadog_api_client.v2.model.http_log_item import HTTPLogItem

# project
from cache.common import read_cache
from cache.env import CONTROL_PLANE_ID_SETTING, DD_API_KEY_SETTING, DD_TELEMETRY_SETTING, LOG_LEVEL_SETTING, is_truthy
from tasks.common import now

log = getLogger(__name__)

# silence azure logging except for errors
getLogger("azure").setLevel(ERROR)

IGNORED_LOG_EXTRAS = {"created", "relativeCreated", "thread", "args"}


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


class Task(AbstractAsyncContextManager["Task"]):
    NAME: str

    def __init__(self) -> None:
        self.credential = DefaultAzureCredential()

        # Telemetry Logic
        self.execution_id = str(uuid4())
        self.control_plane_id = environ.get(CONTROL_PLANE_ID_SETTING, "unknown")
        tags = ["forwarder:lfocontrolplane", f"task:{self.NAME}", f"control_plane_id:{self.control_plane_id}"]
        self.dd_tags = ",".join(tags)
        self.telemetry_enabled = bool(is_truthy(DD_TELEMETRY_SETTING) and environ.get(DD_API_KEY_SETTING))
        self.log = log.getChild(self.__class__.__name__)
        self._logs: list[LogRecord] = []
        self._datadog_client = AsyncApiClient(Configuration())
        self._logs_client = LogsApi(self._datadog_client)
        if self.telemetry_enabled:
            log.info("Telemetry enabled, will submit logs for %s", self.NAME)
            self.log.addHandler(ListHandler(self._logs))

    @abstractmethod
    async def run(self) -> None: ...

    async def __aenter__(self) -> Self:
        await gather(self.credential.__aenter__(), self._datadog_client.__aenter__())
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        submit_telemetry = create_task(self.submit_telemetry())
        if exc_type is None and exc_value is None and traceback is None:
            await self.write_caches()
        await self.credential.__aexit__(exc_type, exc_value, traceback)
        await submit_telemetry
        await self._datadog_client.__aexit__(exc_type, exc_value, traceback)

    @abstractmethod
    async def write_caches(self) -> None: ...

    async def submit_telemetry(self) -> None:
        if not self.telemetry_enabled or not self._logs:
            return
        dd_logs = [
            HTTPLogItem(
                **{k: str(v) for k, v in record.__dict__.items() if k not in IGNORED_LOG_EXTRAS},
                message=record.getMessage(),
                ddsource="azure",
                service="lfo",
                time=record.asctime,
                level=record.levelname,
                execution_id=self.execution_id,
                control_plane_id=self.control_plane_id,
                task=self.NAME,
                **get_error_telemetry(record.exc_info),
            )
            for record in self._logs
        ]
        self._logs.clear()
        await self._logs_client.submit_log(HTTPLog(value=dd_logs), ddtags=self.dd_tags)  # type: ignore


async def task_main(task_class: type[Task], caches: list[str]) -> None:
    level = environ.get(LOG_LEVEL_SETTING, "INFO").upper()
    if level not in {"ERROR", "WARN", "WARNING", "INFO", "DEBUG"}:
        level = "INFO"
    basicConfig()
    log.setLevel(level)
    log.info("Started %s at %s (log level %s)", task_class.NAME, now(), level)
    cache_states = await gather(*map(read_cache, caches))
    async with task_class(*cache_states) as task:
        await task.run()
    log.info("%s finished at %s", task_class.NAME, now())
