# stdlib
from copy import deepcopy
from datetime import datetime
from json import JSONDecodeError, dumps, loads
from logging import INFO, WARNING, getLogger
from typing import AsyncContextManager, TypedDict


# 3p
from azure.functions import FunctionApp, TimerRequest, Out
from azure.identity.aio import DefaultAzureCredential
from jsonschema import ValidationError, validate

# silence azure logging except for warnings
getLogger("azure").setLevel(WARNING)


EVENT_HUB_SCALING_TASK_NAME = "event_hub_scaling_task"
BLOB_STORAGE_CACHE = "resources-cache"
STORAGE_CONNECTION_SETTING = "AzureWebJobsStorage"


log = getLogger(EVENT_HUB_SCALING_TASK_NAME)
log.setLevel(INFO)


class EventHubScalingCache(TypedDict):
    event_hubs: dict[str, dict[str, dict[str, str]]]
    "mapping of subscription id -> region -> event hub id -> namespace"
    resource_assignments: dict[str, dict[str, tuple[str, str]]]
    "mapping of subscription id -> resource id -> (region, event hub id)"


INVALID_CACHE_MSG = "Cache is in an invalid format, task will reset the cache"


UUID_REGEX = r"^[a-f0-9]{8}-([a-f0-9]{4}-){3}[a-f0-9]{12}$"

EVENT_HUB_SCALING_CACHE_SCHEMA = {
    "type": "object",
    "properties": {
        "event_hubs": {
            "type": "object",
            "patternProperties": {
                UUID_REGEX: {
                    "type": "object",
                    "patternProperties": {
                        ".*": {"type": "object", "patternProperties": {".*": {"type": "string"}}},
                    },
                },
            },
        },
        "resource_assignments": {
            "type": "object",
            "patternProperties": {
                UUID_REGEX: {
                    "type": "object",
                    "patternProperties": {
                        ".*": {
                            "type": "array",
                            "items": {"type": "string"},
                            "minItems": 2,
                            "maxItems": 2,
                        },
                    },
                },
            },
        },
    },
}


def deserialize_cache(cache_str: str) -> EventHubScalingCache:
    try:
        cache = loads(cache_str)
        validate(instance=cache, schema=EVENT_HUB_SCALING_CACHE_SCHEMA)
        return cache
    except (JSONDecodeError, ValidationError):
        log.warning(INVALID_CACHE_MSG)
        raise NotImplementedError("TODO: figure out how to restore the cache from a broken state")


class EventHubScalingTask(AsyncContextManager):
    def __init__(self, cache_initial_state: str, cache: Out[str], resource_cache: str) -> None:
        self.credential = DefaultAzureCredential()
        self._event_hub_cache_initial_state = deserialize_cache(cache_initial_state)
        self.event_hub_cache = deepcopy(self._event_hub_cache_initial_state)
        self._raw_cache = cache

    async def run(self) -> None:
        pass

    async def __aexit__(self, *_) -> None:
        self._write_cache()
        await self.credential.close()

    def _write_cache(self) -> None:
        if self.event_hub_cache == self._event_hub_cache_initial_state:
            log.info("Cache has not changed, skipping write")
            return
        log.info("Writing cache to blob storage")
        self._raw_cache.set(dumps(self._event_hub_cache))


def now() -> str:
    return datetime.now().isoformat()


app = FunctionApp()


@app.function_name(name=EVENT_HUB_SCALING_TASK_NAME)
@app.schedule(schedule="0 */5 * * * *", arg_name="req", run_on_startup=False)
@app.blob_input(
    arg_name="eventHubScalingCacheState",  # snake case is illegal in arg names
    path=BLOB_STORAGE_CACHE + "/event_hub_scaling.json",
    connection=STORAGE_CONNECTION_SETTING,
)
@app.blob_input(
    arg_name="resourceCacheState",
    path=BLOB_STORAGE_CACHE + "/resources.json",
    connection=STORAGE_CONNECTION_SETTING,
)
@app.blob_output(
    arg_name="eventHubScalingCache",
    path=BLOB_STORAGE_CACHE + "/event_hub_scaling.json",
    connection=STORAGE_CONNECTION_SETTING,
)
async def run_job(
    req: TimerRequest, eventHubScalingCacheState: str, eventHubScalingCache: Out[str], resourceCacheState: str
) -> None:
    if req.past_due:
        log.info("The task is past due!")
    log.info("Started task at %s", now())
    async with EventHubScalingTask(eventHubScalingCacheState, eventHubScalingCache, resourceCacheState) as task:
        await task.run()
    log.info("Task finished at %s", now())
