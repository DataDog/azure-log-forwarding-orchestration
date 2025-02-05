# stdlib
from abc import abstractmethod
from asyncio import create_task, gather
from contextlib import AbstractAsyncContextManager
from logging import ERROR, Formatter, Handler, LogRecord, getLogger
from os import environ
from types import TracebackType
from typing import Self

# 3p
from azure.identity.aio import DefaultAzureCredential
from datadog_api_client import AsyncApiClient, Configuration
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.http_log import HTTPLog
from datadog_api_client.v2.model.http_log_item import HTTPLogItem

# project
from cache.env import CONTROL_PLANE_ID_SETTING, DD_API_KEY_SETTING, DD_TELEMETRY_SETTING, is_truthy

log = getLogger(__name__)

# silence azure logging except for errors
getLogger("azure").setLevel(ERROR)

LOG_FMT = "%(asctime)s %(levelname)s [%(name)s][%(filename)s:%(lineno)d] %(message)s"


class ListHandler(Handler):
    """A logging handler that appends log messages to a list"""

    def __init__(self, logs: list[str]):
        super().__init__()
        self.log_list = logs
        self.setFormatter(Formatter(LOG_FMT))

    def emit(self, record: LogRecord) -> None:
        self.log_list.append(self.format(record))


class Task(AbstractAsyncContextManager["Task"]):
    NAME: str

    def __init__(self) -> None:
        self.credential = DefaultAzureCredential()

        # Telemetry Logic
        tags = ["forwarder:lfocontrolplane", f"task:{self.NAME}"]
        if control_plane_id := environ.get(CONTROL_PLANE_ID_SETTING):
            tags.append(f"control_plane_id:{control_plane_id}")
        self.dd_tags = ",".join(tags)
        self.telemetry_enabled = bool(is_truthy(DD_TELEMETRY_SETTING) and environ.get(DD_API_KEY_SETTING))
        self.log = log.getChild(self.__class__.__name__)
        self._logs: list[str] = []
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

    @abstractmethod
    async def write_caches(self) -> None: ...

    async def submit_telemetry(self) -> None:
        if not self.telemetry_enabled or not self._logs:
            return
        dd_logs = [
            HTTPLogItem(
                message=message,
                ddsource="azure",
                service="lfo",
            )
            for message in self._logs
        ]
        self._logs.clear()
        await self._logs_client.submit_log(HTTPLog(value=dd_logs), ddtags=self.dd_tags)  # type: ignore
