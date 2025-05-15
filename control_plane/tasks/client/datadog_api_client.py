# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from enum import Enum
from types import TracebackType
from typing import Self

# 3p
import aiohttp


class StatusCode(Enum):
    OK = 200
    DESERIALIZATION_ERROR = 502
    RESOURCE_CREATION_ERROR = 503
    AZURE_RESPONSE_ERROR = 555
    UNKNOWN_ERROR = 999


def create_submit_status_payload(
    step: str,
    status: StatusCode,
    message: str,
    execution_id: str,
    version: str,
    control_plane_id: str,
):
    return {
        "id": execution_id,
        "type": "workflow_id",
        "attributes": {
            "workflow_type": "add_azure_log_forwarder",
            "step_id": step,
            "status": status.name,
            "message": message,
            "metadata": {
                "version": version,
                "control_plane_id": control_plane_id,
            },
        },
    }


class DatadogClient:
    def __init__(self, dd_site: str | None, api_key: str | None):
        self.dd_site = dd_site
        self.api_key = api_key
        self.session = None

    def _get_url(self, endpoint: str) -> str:
        return f"https://api.{self.dd_site}/{endpoint}"

    def _get_headers(self) -> dict:
        return {"dd-api-key": self.api_key, "Content-Type": "application/json"}

    async def submit_status_update(
        self,
        step: str,
        status: StatusCode,
        message: str,
        execution_id: str,
        version: str,
        control_plane_id: str | None,
    ) -> int:
        url = self._get_url("api/unstable/integration/azure/logforwarding/status")
        payload = create_submit_status_payload(
            step=step,
            status=status,
            message=message,
            execution_id=execution_id,
            version=version,
            control_plane_id=control_plane_id,
        )
        async with self.session.post(url, json={"data": payload}, headers=self._get_headers()) as response:  # type: ignore
            return response.status

    async def __aenter__(self) -> Self:
        self.session = aiohttp.ClientSession()
        await self.session.__aenter__()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        await self.session.__aexit__(exc_type, exc_value, traceback)  # type: ignore
