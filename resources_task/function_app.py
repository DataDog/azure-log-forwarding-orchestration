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

# silence azure logging except for warnings
getLogger("azure").setLevel(WARNING)


RESOURCES_TASK_NAME = "resources_task"
BLOB_STORAGE_CACHE = "resources-cache"
BLOB_CONNECTION_SETTING_NAME = "BLOB_CONNECTION_STRING"


log = getLogger(RESOURCES_TASK_NAME)
log.setLevel(INFO)


ResourceCache: TypeAlias = dict[str, set[str]]
"mapping of subscription_id to resource_ids"

INVALID_CACHE_MSG = "Cache is in an invalid format, task will reset the cache"


def deserialize_cache(cache_str: str) -> ResourceCache:
    try:
        cache = loads(cache_str)
    except JSONDecodeError:
        log.warning(INVALID_CACHE_MSG)
        return {}
    if not isinstance(cache, dict):
        log.warning(INVALID_CACHE_MSG)
        return {}
    for sub_id, resources in cache.items():
        if not isinstance(resources, list):
            log.warning(INVALID_CACHE_MSG)
            cache = {}
            break
        cache[sub_id] = set(resources)
    return cache


class ResourcesTask:
    def __init__(self, credential: DefaultAzureCredential, cache_initial_state: str, cache: Out[str]) -> None:
        self.credential = credential
        self.resource_cache: ResourceCache = {}
        "in-memory cache of subscription_id to resource_ids"
        self._cache = cache
        self._resource_cache_initial_state: ResourceCache = deserialize_cache(cache_initial_state)

        """Cache of subscription_id to resource_id to diagnostic_setting_id, or None if the resource is new/has no diagnotic setting."""

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
    connection=BLOB_CONNECTION_SETTING_NAME,
)
@app.blob_output(
    arg_name="resourceCache",
    path=BLOB_STORAGE_CACHE + "/resources.json",
    connection=BLOB_CONNECTION_SETTING_NAME,
)
async def run_job(req: TimerRequest, resourceCacheState: str, resourceCache: Out[str]) -> None:
    if req.past_due:
        log.info("The task is past due!")
    log.info("Started task at %s", now())
    async with DefaultAzureCredential() as cred:
        await ResourcesTask(cred, resourceCacheState, resourceCache).run()
    log.info("Task finished at %s", now())
