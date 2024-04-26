# stdlib
from datetime import datetime
from json import JSONDecodeError, loads
from logging import INFO, WARNING, getLogger
from typing import TypedDict


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
        # here we should figure out how to restore the cache
        raise NotImplementedError


class EventHubScalingTask:
    def __init__(self, credential: DefaultAzureCredential, cache_initial_state: str, cache: Out[str]) -> None:
        self.credential = credential
        self._event_hub_cache_initial_state = deserialize_cache(cache_initial_state)

        self._event_hub_cache = cache

    async def run(self) -> None:
        pass


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
@app.blob_output(
    arg_name="eventHubScalingCache",
    path=BLOB_STORAGE_CACHE + "/event_hub_scaling.json",
    connection=STORAGE_CONNECTION_SETTING,
)
async def run_job(req: TimerRequest, eventHubScalingCacheState: str, eventHubScalingCache: Out[str]) -> None:
    if req.past_due:
        log.info("The task is past due!")
    log.info("Started task at %s", now())
    async with DefaultAzureCredential() as cred:
        await EventHubScalingTask(cred, eventHubScalingCacheState, eventHubScalingCache).run()
    log.info("Task finished at %s", now())
