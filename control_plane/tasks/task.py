# stdlib
from abc import abstractmethod
from contextlib import AbstractAsyncContextManager
from logging import ERROR, getLogger
from typing import Self

# 3p
from azure.identity.aio import DefaultAzureCredential

log = getLogger(__name__)

# silence azure logging except for errors
getLogger("azure").setLevel(ERROR)


class Task(AbstractAsyncContextManager):
    def __init__(self) -> None:
        self.credential = DefaultAzureCredential()

    @abstractmethod
    async def run(self) -> None: ...

    async def __aenter__(self) -> Self:
        await self.credential.__aenter__()
        return self

    async def __aexit__(self, *_) -> None:
        await self.write_caches()
        await self.credential.__aexit__()

    @abstractmethod
    async def write_caches(self) -> None: ...
