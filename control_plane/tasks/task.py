# stdlib
from abc import abstractmethod
from contextlib import AbstractAsyncContextManager
from logging import ERROR, getLogger
from types import TracebackType
from typing import Self

# 3p
from azure.identity.aio import DefaultAzureCredential

log = getLogger(__name__)

# silence azure logging except for errors
getLogger("azure").setLevel(ERROR)


class Task(AbstractAsyncContextManager["Task"]):
    def __init__(self) -> None:
        self.credential = DefaultAzureCredential()

    @abstractmethod
    async def run(self) -> None: ...

    async def __aenter__(self) -> Self:
        await self.credential.__aenter__()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        if exc_type is None and exc_value is None and traceback is None:
            await self.write_caches()
            await self.write_audit_logs()
        await self.credential.__aexit__(exc_type, exc_value, traceback)

    @abstractmethod
    async def write_caches(self) -> None: ...

    @abstractmethod
    async def write_audit_logs(self) -> None: ...
