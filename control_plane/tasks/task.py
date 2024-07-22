# stdlib
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import AsyncContextManager, Self, TypeVar, Any
from logging import ERROR, getLogger

# 3p
from azure.identity.aio import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.core.polling import AsyncLROPoller
from tenacity import retry, stop_after_delay

log = getLogger(__name__)

# silence azure logging except for errors
getLogger("azure").setLevel(ERROR)


def now() -> str:
    """Return the current time in ISO format"""
    return datetime.now().isoformat()


T = TypeVar("T")


async def wait_for_resource(
    poller: AsyncLROPoller[T], confirm: Callable[[], Awaitable[Any]], wait_seconds: int = 30
) -> T:
    """Wait for the poller to complete and confirm the resource exists, if the resource does not exist, `confirm` should throw a ResourceNotFoundError"""
    res = await poller.result()

    @retry(stop=stop_after_delay(wait_seconds))
    async def confirm_exists() -> None:
        try:
            await confirm()
        except ResourceNotFoundError:
            raise
        except Exception:
            log.exception("Unexpected error while confirming resource exists")
            return

    await confirm_exists()
    return res


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
