# stdlib
from asyncio import Task, create_task, gather
from datetime import datetime, timezone
from json import dumps, loads
from logging import INFO, WARNING, getLogger
from typing import Any, AsyncIterable, Callable, NotRequired, TypeAlias, TypedDict


# 3p
from azure.functions import FunctionApp, TimerRequest, Out
from azure.mgmt.resource.subscriptions.aio import SubscriptionClient
from azure.mgmt.resource.resources.aio import ResourceManagementClient
from azure.mgmt.storage.v2023_01_01.aio import StorageManagementClient
from azure.mgmt.storage.v2023_01_01.models import Resource
from azure.identity.aio import DefaultAzureCredential

# silence azure logging except for warnings
getLogger("azure").setLevel(WARNING)


RESOURCES_TASK_NAME = "resources_task"
BLOB_STORAGE_CACHE = "resources-cache"
BLOB_CONNECTION_SETTING_NAME = "BLOB_CONNECTION_STRING"


log = getLogger(RESOURCES_TASK_NAME)
log.setLevel(INFO)


class ResourceConfiguration(TypedDict):
    diagnostic_setting_id: NotRequired[str]
    event_hub_name: NotRequired[str]
    event_hub_namespace: NotRequired[str]


SubscriptionId: TypeAlias = str
ResourceId: TypeAlias = str
ResourceCache: TypeAlias = dict[SubscriptionId, dict[ResourceId, ResourceConfiguration]]


class ResourcesTask:

    def __init__(self, credential: DefaultAzureCredential, resources: str, cache: Out[str]) -> None:
        self.credential = credential
        self.resource_ids_per_subscription: dict[str, set[str]] = {}
        self._cache = cache
        self._resources_cache_initial_state: ResourceCache = loads(resources) if resources else {}
        """Cache of subscription_id to resource_id to diagnostic_setting_id, or None if the resource is new/has no diagnotic setting."""

    async def run(self) -> None:
        async with SubscriptionClient(self.credential) as subscription_client:
            await gather(
                *[
                    self.process_subscription(sub.subscription_id)  # type: ignore
                    async for sub in subscription_client.subscriptions.list()
                ]
            )
        self.store_resources()

    async def process_subscription(self, subscription_id: str) -> None:
        log.info("Processing the following subscription: %s", subscription_id)
        subresource_tasks: list[Task[None]] = []
        resource_ids: set[str] = set()

        async def get_and_store_sub_resource_ids(
            call: Callable[..., AsyncIterable[Resource]], *args: Any, **kwargs: Any
        ):
            resource_ids.update([resource.id async for resource in call(*args, **kwargs)])  # type: ignore

        async with (
            ResourceManagementClient(self.credential, subscription_id) as resource_client,
            StorageManagementClient(self.credential, subscription_id) as storage_client,
        ):
            async for resource in resource_client.resources.list():
                resource_ids.add(resource.id)  # type: ignore

                if resource.type == "Microsoft.Storage/storageAccounts":
                    resource_group = resource.id.split("/")[4]  # type: ignore
                    subresource_tasks.append(
                        create_task(
                            get_and_store_sub_resource_ids(
                                storage_client.blob_containers.list, resource_group, resource.name
                            )
                        )
                    )
            await gather(*subresource_tasks)

        log.info(f"Subscription {subscription_id}: Collected {len(resource_ids)} resources")

        # async with
        self.resource_ids_per_subscription[subscription_id] = resource_ids

    def store_resources(self) -> None:
        if not isinstance(self._resources_cache_initial_state, dict):
            log.warning("Cache is in an invalid format, resetting the cache")
            self._cache.set(
                dumps(
                    {
                        sub_id: {resource_id: {} for resource_id in resource_ids}
                        for sub_id, resource_ids in self.resource_ids_per_subscription.items()
                    }
                )
            )
            return

        new_cache: ResourceCache = {
            subscription_id: {
                resource_id: self._resources_cache_initial_state.get(subscription_id, {}).get(resource_id, {})
                for resource_id in resource_ids
            }
            for subscription_id, resource_ids in self.resource_ids_per_subscription.items()
        }
        if new_cache != self._resources_cache_initial_state:
            self._cache.set(dumps(new_cache))
            log.info(f"Updated Resources, {len(new_cache)} resources stored in the cache")
        else:
            log.info(f"Resources have not changed, no update needed")


def now() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()


app = FunctionApp()


@app.function_name(name=RESOURCES_TASK_NAME)
@app.schedule(schedule="0 */5 * * * *", arg_name="req", run_on_startup=False)
@app.blob_input(
    arg_name="resources",
    path=BLOB_STORAGE_CACHE + "/resources.json",
    connection=BLOB_CONNECTION_SETTING_NAME,
)
@app.blob_output(
    arg_name="cache",
    path=BLOB_STORAGE_CACHE + "/resources.json",
    connection=BLOB_CONNECTION_SETTING_NAME,
)
async def run_job(req: TimerRequest, resources: str, cache: Out[str]) -> None:
    if req.past_due:
        log.info("The task is past due!")
    log.info("Started task at %s", now())
    async with DefaultAzureCredential() as cred:
        await ResourcesTask(cred, resources, cache).run()
    log.info("Task finished at %s", now())
