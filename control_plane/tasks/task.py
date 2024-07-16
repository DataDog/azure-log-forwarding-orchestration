# stdlib
from abc import ABC, abstractmethod
from datetime import datetime
from typing import AsyncContextManager, Self
from logging import ERROR, getLogger

# 3p
from azure.identity.aio import DefaultAzureCredential


# silence azure logging except for errors
getLogger("azure").setLevel(ERROR)


def now() -> str:
    """Return the current time in ISO format"""
    return datetime.now().isoformat()


class Task(AsyncContextManager, ABC):
    def __init__(self) -> None:
        self.credential = DefaultAzureCredential()

    @abstractmethod
    async def run(self) -> None: ...

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        await self.write_caches()
        await self.credential.close()

    @abstractmethod
    async def write_caches(self) -> None: ...
