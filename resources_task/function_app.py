# stdlib
from asyncio import gather
from datetime import datetime
from json import JSONDecodeError, dumps, loads
from logging import INFO, WARNING, getLogger
from typing import TypeAlias, cast


# 3p
from azure.functions import FunctionApp, TimerRequest, Out
from azure.mgmt.resource.subscriptions.v2021_01_01.aio import SubscriptionClient
from azure.mgmt.resource.resources.v2021_01_01.aio import ResourceManagementClient
from azure.identity.aio import DefaultAzureCredential
from jsonschema import ValidationError, validate

# silence azure logging except for warnings
getLogger("azure").setLevel(WARNING)


RESOURCES_TASK_NAME = "resources_task"
BLOB_STORAGE_CACHE = "resources-cache"
STORAGE_CONNECTION_SETTING = "AzureWebJobsStorage"


log = getLogger(RESOURCES_TASK_NAME)
log.setLevel(INFO)


class ResourcesTask:
    def __init__(
        self, credential: DefaultAzureCredential, cache_initial_state: str, resources_cache_out: Out[str]
    ) -> None:
        self.credential = credential
        self._cache = resources_cache_out
        success, resource_cache = deserialize_resource_cache(cache_initial_state)
        if not success:
            log.warning("Resource Cache is in an invalid format, task will reset the cache")
            resource_cache = {}
        self._resource_cache_initial_state = resource_cache

        self.resource_cache: ResourceCache = {}
        "in-memory cache of subscription_id to resource_ids"

    async def run(self) -> None:
        async with SubscriptionClient(self.credential) as subscription_client:
            await gather(
                *[
                    self.process_subscription(cast(str, sub.subscription_id))
                    async for sub in subscription_client.subscriptions.list()
                ]
            )
        self.store_resources()

    async def process_subscription(self, subscription_id: str) -> None:
        log.info("Processing the following subscription: %s", subscription_id)
        async with ResourceManagementClient(self.credential, subscription_id) as client:
            resource_ids: set[str] = {cast(str, r.id) async for r in client.resources.list()}
            log.info(f"Subscription {subscription_id}: Collected {len(resource_ids)} resources")
            self.resource_cache[subscription_id] = resource_ids

    def store_resources(self) -> None:
        if self.resource_cache != self._resource_cache_initial_state:
            # since sets cannot be json serialized, we convert them to lists before storing
            self._cache.set(dumps(self.resource_cache, default=list))
            resources_count = sum(len(resources) for resources in self.resource_cache.values())
            log.info(f"Updated Resources, {resources_count} resources stored in the cache")
        else:
            log.info("Resources have not changed, no update needed")


def now() -> str:
    return datetime.now().isoformat()


app = FunctionApp()


@app.function_name(name=RESOURCES_TASK_NAME)
@app.schedule(schedule="0 */5 * * * *", arg_name="req", run_on_startup=False)
@app.blob_input(
    arg_name="resourceCacheState",  # snake case is illegal in arg names
    path=BLOB_STORAGE_CACHE + "/resources.json",
    connection=STORAGE_CONNECTION_SETTING,
)
@app.blob_output(
    arg_name="resourceCache",
    path=BLOB_STORAGE_CACHE + "/resources.json",
    connection=STORAGE_CONNECTION_SETTING,
)
async def run_job(req: TimerRequest, resourceCacheState: str, resourceCache: Out[str]) -> None:
    if req.past_due:
        log.info("The task is past due!")
    log.info("Started task at %s", now())
    async with DefaultAzureCredential() as cred:
        await ResourcesTask(cred, resourceCacheState, resourceCache).run()
    log.info("Task finished at %s", now())


### cache/resources_cache.py

UUID_REGEX = r"^[a-f0-9]{8}-([a-f0-9]{4}-){3}[a-f0-9]{12}$"

ResourceCache: TypeAlias = dict[str, set[str]]
"mapping of subscription_id to resource_ids"


RESOURCE_CACHE_SCHEMA = {
    "type": "object",
    "patternProperties": {
        UUID_REGEX: {"type": "array", "items": {"type": "string"}},
    },
}


def deserialize_resource_cache(cache_str: str) -> tuple[bool, ResourceCache]:
    """Deserialize the resource cache, returning a tuple of success and the cache dict."""
    try:
        cache = loads(cache_str)
        validate(instance=cache, schema=RESOURCE_CACHE_SCHEMA)
        return True, {sub_id: set(resources) for sub_id, resources in cache.items()}
    except (JSONDecodeError, ValidationError):
        return False, {}
