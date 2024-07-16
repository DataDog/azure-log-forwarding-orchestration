# stdlib
from abc import ABC, abstractmethod
from datetime import datetime
from os import environ
from typing import AsyncContextManager, Self
from logging import ERROR, getLogger

# 3p
from azure.identity.aio import DefaultAzureCredential


# silence azure logging except for errors
getLogger("azure").setLevel(ERROR)


class MissingConfigOptionError(Exception):
    def __init__(self, option: str) -> None:
        super().__init__(f"Missing required configuration option: {option}")


def get_config_option(name: str) -> str:
    """Get a configuration option from the environment or raise a helpful error"""
    if option := environ.get(name):
        return option
    raise MissingConfigOptionError(name)


def now() -> str:
    """Return the current time in ISO format"""
    return datetime.now().isoformat()


class Task(AsyncContextManager, ABC):
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
