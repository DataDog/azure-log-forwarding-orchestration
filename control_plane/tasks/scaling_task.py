# stdlib
from asyncio import gather, run
from collections.abc import Generator, Iterable
from copy import deepcopy
from datetime import datetime, timedelta
from itertools import chain
from json import dumps
from logging import DEBUG, INFO, basicConfig, getLogger
from typing import cast

# 3p
from tenacity import retry, retry_if_result, stop_after_attempt

# project
from cache.assignment_cache import (
    ASSIGNMENT_CACHE_BLOB,
    AssignmentCache,
    RegionAssignmentConfiguration,
    deserialize_assignment_cache,
)
from cache.common import (
    InvalidCacheError,
    LogForwarder,
    get_config_option,
    parse_config_option,
    read_cache,
    write_cache,
)
from cache.metric_blob_cache import MetricBlobEntry
from cache.resources_cache import RESOURCE_CACHE_BLOB, ResourceCache, deserialize_resource_cache
from tasks.client.log_forwarder_client import LogForwarderClient
from tasks.common import average, chunks, generate_unique_id, log_errors, now
from tasks.constants import ALLOWED_CONTAINER_APP_REGIONS
from tasks.task import Task

SCALING_TASK_NAME = "scaling_task"

SCALING_METRIC_PERIOD_MINUTES = 5
DELETION_METRIC_PERIOD_MINUTES = 15
METRIC_COLLECTION_PERIOD_MINUTES = DELETION_METRIC_PERIOD_MINUTES  # longer of the two periods^
DEFAULT_SCALING_PERCENTAGE = 0.8

SCALE_UP_EXECUTION_SECONDS = 45
SCALE_DOWN_EXECUTION_SECONDS = 3

log = getLogger(SCALING_TASK_NAME)
log.setLevel(DEBUG)


def is_consistently_over_threshold(metrics: list[MetricBlobEntry], threshold: float, percentage: float) -> bool:
    """Check if the runtime is consistently over the threshold.
    percentage is a float between 0 and 1, representing the percentage of metrics that need to exceed the threshold
    """
    if not metrics:
        return False
    exceeded_metrics = [metric for metric in metrics if metric["runtime_seconds"] > threshold]
    return float(len(exceeded_metrics)) / len(metrics) > percentage


def is_consistently_under_threshold(metrics: list[MetricBlobEntry], threshold: float, percentage: float) -> bool:
    """Check if the runtime is consistently under the threshold
    percentage is a float between 0 and 1, representing the percentage of metrics that need to be under the threshold
    """
    if not metrics:
        return False
    under_threshold_metrics = [metric for metric in metrics if metric["runtime_seconds"] < threshold]
    return float(len(under_threshold_metrics)) / len(metrics) > percentage


def resources_to_move_by_load(resource_loads: dict[str, int]) -> Generator[str, None, None]:
    half_load = sum(resource_loads.values()) / 2
    load_so_far = 0

    def _sort_key(kv: tuple[str, int]) -> tuple[int, str]:
        """Sort by load, then alphabetically if we have a tie"""
        return kv[1], kv[0]

    for resource, load in sorted(resource_loads.items(), key=_sort_key):
        load_so_far += load
        if load_so_far > half_load:
            yield resource


def prune_assignment_cache(resource_cache: ResourceCache, assignment_cache: AssignmentCache) -> AssignmentCache:
    """Updates the assignment cache based on any deletions in the resource cache"""
    assignment_cache = deepcopy(assignment_cache)

    def _prune_region_config(subscription_id: str, region: str) -> RegionAssignmentConfiguration:
        resources = resource_cache.get(subscription_id, {}).get(region, set())
        current_region_config = assignment_cache.get(subscription_id, {}).get(
            region,
            {"configurations": {}, "resources": {}},  # default empty region config
        )
        current_region_config["resources"] = {
            resource_id: config_id
            for resource_id in resources
            if (config_id := current_region_config["resources"].get(resource_id))
        }
        return current_region_config

    pruned_cache = {
        sub_id: {region: _prune_region_config(sub_id, region) for region in region_resources}
        for sub_id, region_resources in resource_cache.items()
    }

    # add any regions that are in the assignment cache but not in the resource cache
    for sub_id, region_configs in assignment_cache.items():
        for region, config in region_configs.items():
            if region not in pruned_cache.get(sub_id, {}):
                # clear just the resources, we still have forwarders to clean up
                pruned_cache.setdefault(sub_id, {})[region] = {**config, "resources": {}}

    return pruned_cache


class ScalingTask(Task):
    def __init__(self, resource_cache_state: str, assignment_cache_state: str) -> None:
        super().__init__()
        self.resource_group = get_config_option("RESOURCE_GROUP")
        self.scaling_percentage = parse_config_option("SCALING_PERCENTAGE", float, DEFAULT_SCALING_PERCENTAGE)
        self.control_plane_region = get_config_option("CONTROL_PLANE_REGION")

        self.now = datetime.now()

        # Resource Cache
        resource_cache = deserialize_resource_cache(resource_cache_state)
        if resource_cache is None:
            raise InvalidCacheError("Resource Cache is in an invalid format, failing this task until it is valid")
        self.resource_cache = resource_cache

        # Assignment Cache
        assignment_cache = deserialize_assignment_cache(assignment_cache_state)
        if assignment_cache is None:
            log.warning("Assignment Cache is in an invalid format, task will reset the cache")
            assignment_cache = {}
        self._assignment_cache_initial_state = assignment_cache
        self.assignment_cache = prune_assignment_cache(resource_cache, assignment_cache)

    async def run(self) -> None:
        log.info("Running for %s subscriptions: %s", len(self.resource_cache), list(self.resource_cache.keys()))
        all_subscriptions = set(self.resource_cache.keys()) | set(self._assignment_cache_initial_state.keys())
        await gather(*(self.process_subscription(sub_id) for sub_id in all_subscriptions))

    async def process_subscription(self, subscription_id: str) -> None:
        regions_with_forwarders = {
            region
            for region, region_config in self._assignment_cache_initial_state.get(subscription_id, {}).items()
            if region_config.get("configurations")
        }
        regions_with_resources = set(self.resource_cache.get(subscription_id, {}).keys())
        # regions with any forwarders or environments
        provisioned_regions = set(self._assignment_cache_initial_state.get(subscription_id, {}).keys())

        regions_to_add = regions_with_resources - regions_with_forwarders
        regions_to_remove = provisioned_regions - regions_with_resources
        regions_to_check_scaling = regions_with_resources & regions_with_forwarders
        async with LogForwarderClient(self.credential, subscription_id, self.resource_group) as client:
            await gather(
                *(self.set_up_region(client, subscription_id, region) for region in regions_to_add),
                *(self.delete_region(client, subscription_id, region) for region in regions_to_remove),
                *(
                    self.maintain_existing_region(client, subscription_id, region)
                    for region in regions_to_check_scaling
                ),
            )
            await self.clean_up_orphaned_forwarders(client, subscription_id)

    @retry(stop=stop_after_attempt(3), retry=retry_if_result(lambda result: result is None))
    async def create_log_forwarder(self, client: LogForwarderClient, region: str) -> LogForwarder | None:
        """Creates a log forwarder for the given subscription and region and returns the configuration id and type.
        Will try 3 times, and if the creation fails, the forwarder is (attempted to be) deleted and None is returned.
        If container apps are not supported in the region, the forwarder is created in the same region as the control plane."""
        config_id = generate_unique_id()
        try:
            config_type = await client.create_log_forwarder(region, config_id)
            return LogForwarder(config_id, config_type)
        except Exception:
            log.exception("Failed to create log forwarder %s, cleaning up", config_id)
            success = await client.delete_log_forwarder(config_id, raise_error=False)
            if not success:
                log.error("Failed to clean up log forwarder %s, manual intervention required", config_id)
            return None

    async def create_log_forwarder_env(self, client: LogForwarderClient, region: str) -> None:
        """Creates a log forwarder env for the given subscription and region and returns the resource id.
        If the creation fails, the forwarder is (attempted to be) deleted and None is returned"""
        try:
            await client.create_log_forwarder_managed_environment(region)
        except Exception:
            log.exception("Failed to create log forwarder env for region %s, cleaning up", region)
            success = await client.delete_log_forwarder_env(region, raise_error=False)
            if not success:
                log.error("Failed to clean up log forwarder env for region %s, manual intervention required", region)

    async def set_up_region(
        self,
        client: LogForwarderClient,
        subscription_id: str,
        region: str,
    ) -> None:
        """Creates a log forwarder for the given subscription and region and assigns resources to it.
        Only done the first time we discover a new region.

        Will never raise an exception.
        """

        env_exists = await self.check_region_forwarder_env(client, region)
        if not env_exists:
            log.info("Creating log forwarder env for subscription %s in region %s", subscription_id, region)
            await self.create_log_forwarder_env(client, region)
            # log forwarder environments take multiple minutes to be ready, so we should wait until the next run
            return

        log_forwarder = await self.create_log_forwarder(client, region)
        if log_forwarder is None:
            return
        config_id, config_type = log_forwarder
        self.assignment_cache.setdefault(subscription_id, {})[region] = {
            "configurations": {config_id: config_type},
            "resources": {resource: config_id for resource in self.resource_cache[subscription_id][region]},
        }
        await self.write_caches()

    async def delete_region(
        self,
        client: LogForwarderClient,
        subscription_id: str,
        region: str,
    ) -> None:
        """Cleans up a region by deleting all log forwarders for the given subscription and region."""
        forwarders = self._assignment_cache_initial_state[subscription_id][region]["configurations"]
        if not forwarders:
            # never delete control plane env. if the region is supported by container apps, it has its own env and should be deleted
            if region != self.control_plane_region and region in ALLOWED_CONTAINER_APP_REGIONS:
                log.info("Deleting log forwarder managed env for subscription %s in region %s", subscription_id, region)
                await client.delete_log_forwarder_env(region, raise_error=False)

            self.assignment_cache.get(subscription_id, {}).pop(region, None)
            await self.write_caches()
            return

        # not needed, but useful to indicate all resources are gone
        self.assignment_cache[subscription_id][region]["resources"].clear()

        forwarder_metrics = await self.collect_region_forwarder_metrics(client, forwarders)
        forwarders_to_delete = [
            forwarder
            for forwarder, metrics in forwarder_metrics.items()
            if not any(m["resource_log_volume"] for m in metrics)
        ]
        if not forwarders_to_delete:
            log.info("Attempted to delete region %s but all forwarders are still receiving logs", region)
            return

        log.info("Deleting log forwarders for subscription %s in region %s", subscription_id, region)
        maybe_errors = await gather(
            *(
                self.delete_log_forwarder(client, self.assignment_cache[subscription_id][region], forwarder_id)
                for forwarder_id in forwarders_to_delete
            ),
            return_exceptions=True,
        )
        log_errors("Failed to delete region", *maybe_errors)
        await self.write_caches()

    async def maintain_existing_region(self, client: LogForwarderClient, subscription_id: str, region: str) -> None:
        """Checks the performance/scaling of a region and determines/performs scaling as needed

        Additionally assigns new resources to the least busy forwarder
        and reassigns resources based on the new scaling"""
        log.info("Checking scaling for log forwarders in region %s", region)
        region_config = self.assignment_cache[subscription_id][region]

        env_exists = await self.check_region_forwarder_env(client, region)
        if not env_exists:
            log.error(
                "Log forwarder env missing for subscription %s in region %s. Setting up new one.",
                subscription_id,
                region,
            )
            await self.create_log_forwarder_env(client, region)
            return

        all_forwarders_exist = await self.ensure_region_forwarders(client, subscription_id, region)
        if not all_forwarders_exist:
            return
        all_forwarder_metrics = await self.collect_region_forwarder_metrics(client, region_config["configurations"])
        if not any(all_forwarder_metrics.values()):
            log.warning("No valid metrics found for forwarders in region %s", region)
            return

        self.onboard_new_resources(subscription_id, region, all_forwarder_metrics)
        await self.write_caches()

        # count the number of resources after we have onboarded new resources
        config_ids = list(region_config["resources"].values())
        num_resources_by_forwarder = {
            config_id: config_ids.count(config_id) for config_id in region_config["configurations"]
        }
        scaling_metric_cutoff = (self.now - timedelta(minutes=METRIC_COLLECTION_PERIOD_MINUTES)).timestamp()
        scaling_forwarder_metrics = {
            f: [m for m in metrics if m["timestamp"] > scaling_metric_cutoff]
            for f, metrics in all_forwarder_metrics.items()
        }

        did_scale = await self.scale_up_forwarders(
            client,
            subscription_id,
            region,
            num_resources_by_forwarder,
            scaling_forwarder_metrics,
        )
        await self.write_caches()
        # if we don't scale up, we can check for scaling down
        if did_scale:
            return

        await self.scale_down_forwarders(
            client, region, region_config, num_resources_by_forwarder, scaling_forwarder_metrics, all_forwarder_metrics
        )
        await self.write_caches()

    async def ensure_region_forwarders(self, client: LogForwarderClient, subscription_id: str, region: str) -> bool:
        """Ensures that all forwarders cache still exist, making the necessary adjustments to `self.assignment_cache`
        if they don't. Returns True if all forwarders exist, False if there are issues

        ASSUMPTION: Assignment cache is pruned before we execute this. (see `prune_assignment_cache`)"""
        region_config = self.assignment_cache[subscription_id][region]
        if not region_config["configurations"]:
            # TODO(AZINTS-2968) we should do as little as possible, probably just exit out and clear the cache
            log.warning("No forwarders found in cache for region %s, recreating", region)
            self.assignment_cache[subscription_id].pop(region)
            await self.write_caches()
            return False
        # fetch log resources
        forwarder_resources_list = await gather(
            *(client.get_forwarder_resources(config_id) for config_id in region_config["configurations"])
        )
        forwarder_resources = dict(zip(region_config["configurations"], forwarder_resources_list, strict=False))

        if all(all(resources) for resources in forwarder_resources.values()):
            # everything is there!
            return True

        # if all forwarders have been deleted, we should delete the region from the cache and exit
        # it will be recreated next time
        if all(not all(resources) for resources in forwarder_resources.values()):
            # TODO(AZINTS-2968) we should just nuke the region and wait until next time
            log.warning("All forwarders gone in region %s", region)
            self.assignment_cache[subscription_id].pop(region)
            await self.write_caches()
            return False

        # if there are some partially missing forwarders, delete them from
        # the cache and move them over to an existing forwarder
        broken_forwarders = {
            config_id for config_id, forwarder_resources in forwarder_resources.items() if not all(forwarder_resources)
        }
        working_forwarder = next(
            config_id for config_id, forwarder_resources in forwarder_resources.items() if all(forwarder_resources)
        )
        region_config["resources"].update(
            {
                resource_id: working_forwarder
                for resource_id, config_id in region_config["resources"].items()
                if config_id in broken_forwarders
            }
        )
        for broken_forwarder in broken_forwarders:
            region_config["configurations"].pop(broken_forwarder)

        await self.write_caches()
        return False

    async def check_region_forwarder_env(self, client: LogForwarderClient, region: str) -> bool:
        """Checks to see if the forwarder env exists for a given region"""
        return bool(await client.get_log_forwarder_managed_environment(region))

    async def collect_region_forwarder_metrics(
        self, client: LogForwarderClient, log_forwarders: Iterable[str]
    ) -> dict[str, list[MetricBlobEntry]]:
        """Collects metrics for all forwarders in a region and returns them as a dictionary by config_id. Returns an empty dict on failure."""
        oldest_metric_timestamp = (self.now - timedelta(minutes=METRIC_COLLECTION_PERIOD_MINUTES)).timestamp()
        maybe_metrics = await gather(
            *(client.collect_forwarder_metrics(config_id, oldest_metric_timestamp) for config_id in log_forwarders),
            return_exceptions=True,
        )
        errors = log_errors("Failed to collect metrics for forwarders", *maybe_metrics, reraise=False)
        if errors:
            return {}
        return dict(zip(log_forwarders, cast(list[list[MetricBlobEntry]], maybe_metrics), strict=False))

    def onboard_new_resources(
        self, subscription_id: str, region: str, forwarder_metrics: dict[str, list[MetricBlobEntry]]
    ) -> None:
        """Assigns new resources to the least busy forwarder in the region, and updates the cache state accordingly"""
        new_resources = set(self.resource_cache[subscription_id][region]) - set(
            self.assignment_cache[subscription_id][region]["resources"]
        )
        if not new_resources:
            return

        least_busy_forwarder_id = min(
            # any forwarders without metrics we should not add more resources to, there may be something wrong:
            filter(lambda forwarder_id: forwarder_metrics[forwarder_id], forwarder_metrics),
            # find forwarder with the min average runtime
            key=lambda forwarder_id: average(
                *(metric["runtime_seconds"] for metric in forwarder_metrics[forwarder_id])
            ),
        )

        self.assignment_cache[subscription_id][region]["resources"].update(
            {resource: least_busy_forwarder_id for resource in new_resources}
        )

    async def scale_up_forwarders(
        self,
        client: LogForwarderClient,
        subscription_id: str,
        region: str,
        num_resources_by_forwarder: dict[str, int],
        forwarder_metrics: dict[str, list[MetricBlobEntry]],
    ) -> bool:
        def _has_enough_resources_to_scale_up(config_id: str) -> bool:
            if num_resources_by_forwarder[config_id] < 2:
                log.warning("Forwarder %s only has one resource but is overwhelmed", config_id)
                return False
            return True

        forwarders_to_scale_up = [
            config_id
            for config_id, metrics in forwarder_metrics.items()
            if is_consistently_over_threshold(metrics, SCALE_UP_EXECUTION_SECONDS, self.scaling_percentage)
            and _has_enough_resources_to_scale_up(config_id)
        ]

        if not forwarders_to_scale_up:
            return False

        # create a second forwarder for each forwarder that needs to scale up
        new_forwarders = await gather(*[self.create_log_forwarder(client, region) for _ in forwarders_to_scale_up])
        if new_forwarders:
            log.info("Scaled up %s forwarders in region %s", len(new_forwarders), region)

        for overwhelmed_forwarder_id, new_forwarder in zip(forwarders_to_scale_up, new_forwarders, strict=False):
            if not new_forwarder:
                log.warning("Failed to create new log forwarder, skipping scaling for %s", overwhelmed_forwarder_id)
                continue
            self.split_forwarder_resources(
                self.assignment_cache[subscription_id][region],
                overwhelmed_forwarder_id,
                new_forwarder,
                forwarder_metrics[overwhelmed_forwarder_id],
            )

        return True

    def split_forwarder_resources(
        self,
        region_config: RegionAssignmentConfiguration,
        underscaled_forwarder_id: str,
        new_forwarder: LogForwarder,
        metrics: list[MetricBlobEntry],
    ) -> None:
        """Splits the resources of an underscaled forwarder between itself and a new forwarder"""

        # add new config
        region_config["configurations"][new_forwarder.config_id] = new_forwarder.type

        if all(not metric["resource_log_volume"] for metric in metrics):
            log.warning(
                "Resource log volume metrics missing for forwarder %s, falling back to basic splitting",
                underscaled_forwarder_id,
            )
            resources = sorted(
                resource
                for resource, config_id in region_config["resources"].items()
                if config_id == underscaled_forwarder_id
            )
            split_index = len(resources) // 2
            region_config["resources"].update(
                {resource: new_forwarder.config_id for resource in resources[split_index:]}
            )
            return

        # organize resources by resource load
        resource_loads = {
            resource_id: sum(map(lambda m: m["resource_log_volume"].get(resource_id, 0), metrics))
            for resource_id, config_id in region_config["resources"].items()
            if config_id == underscaled_forwarder_id
        }

        # reassign some resources to the new forwarder
        region_config["resources"].update(
            {resource_id: new_forwarder.config_id for resource_id in resources_to_move_by_load(resource_loads)}
        )

    async def scale_down_forwarders(
        self,
        client: LogForwarderClient,
        region: str,
        region_config: RegionAssignmentConfiguration,
        num_resources_by_forwarder: dict[str, int],
        scaling_forwarder_metrics: dict[str, list[MetricBlobEntry]],
        all_forwarder_metrics: dict[str, list[MetricBlobEntry]],
    ) -> None:
        """
        Implements a two phased approach to scaling down forwarders:
        1. Move resources from the forwarder pairs which are overscaled onto just one of them
        2. Delete forwarders which have no resources and are not submitting logs

        These phases will not both happen to the same forwarder in the same run,
        as the requirements are mutually exclusive.

        This is required to ensure we don't delete forwarders that are still receiving logs,
        as doing so would result in log loss.
        """

        # Phase 1: Move resources from the forwarder pairs which are overscaled onto just one of them
        forwarders_to_collapse = sorted(
            [
                config_id
                for config_id, metrics in scaling_forwarder_metrics.items()
                if is_consistently_under_threshold(metrics, SCALE_DOWN_EXECUTION_SECONDS, self.scaling_percentage)
                and num_resources_by_forwarder[config_id] > 0
            ]
        )
        # Phase 2: Delete forwarders with no resources and no metrics
        forwarders_to_delete = [
            config_id
            for config_id, num_resources in num_resources_by_forwarder.items()
            # delete forwarders with no resources and no metrics
            if num_resources == 0
            and (
                not all_forwarder_metrics.get(config_id)
                or all(not m["resource_log_volume"] for m in all_forwarder_metrics[config_id])
            )
        ]

        maybe_errors = await gather(
            *(
                self.collapse_forwarders(region_config, config_1, config_2)
                for config_1, config_2 in chunks(forwarders_to_collapse, 2)
            ),
            *(self.delete_log_forwarder(client, region_config, config_id) for config_id in forwarders_to_delete),
            return_exceptions=True,
        )
        log_errors("Errors during scaling down", *maybe_errors)
        if forwarders_to_delete:
            log.info("Scaled down %s forwarders in region %s", len(forwarders_to_delete), region)

    async def collapse_forwarders(
        self, region_config: RegionAssignmentConfiguration, config_1: str, config_2: str
    ) -> None:
        """Collapses two forwarders into one, moving resources from config_2 to config_1. Deletion of the forwarder will happen once it is empty"""
        resources_to_move = {
            resource_id: config_1
            for resource_id, config_id in region_config["resources"].items()
            if config_id == config_2
        }
        region_config["resources"].update(resources_to_move)

    async def delete_log_forwarder(
        self, client: LogForwarderClient, region_config: RegionAssignmentConfiguration, config_id: str
    ) -> None:
        """Deletes a forwarder and removes it from the configurations"""
        await client.delete_log_forwarder(config_id)
        region_config["configurations"].pop(config_id, None)

    async def clean_up_orphaned_forwarders(self, client: LogForwarderClient, subscription_id: str) -> None:
        existing_log_forwarders = await client.list_log_forwarder_ids()
        orphaned_forwarders = set(existing_log_forwarders) - set(
            chain.from_iterable(
                region_config["configurations"]
                for region_config in self.assignment_cache.get(subscription_id, {}).values()
            )
        )
        if not orphaned_forwarders:
            return

        log.info("Cleaning up orphaned forwarders for subscription %s: %s", subscription_id, orphaned_forwarders)
        await gather(
            *(
                # only try once and don't error, if something transiently fails
                # we can wait til next time, we don't want to spend much time here
                client.delete_log_forwarder(forwarder_id, raise_error=False, max_attempts=1)
                for forwarder_id in orphaned_forwarders
            )
        )

    async def write_caches(self) -> None:
        if self.assignment_cache == self._assignment_cache_initial_state:
            return
        await write_cache(ASSIGNMENT_CACHE_BLOB, dumps(self.assignment_cache))
        log.info("Updated assignments stored in the cache")


async def main() -> None:
    basicConfig(level=INFO)
    log.info("Started task at %s", now())
    resources_cache_state, assignment_cache_state = await gather(
        read_cache(RESOURCE_CACHE_BLOB),
        read_cache(ASSIGNMENT_CACHE_BLOB),
    )
    async with ScalingTask(resources_cache_state, assignment_cache_state) as task:
        await task.run()
    log.info("Task finished at %s", now())


if __name__ == "__main__":  # pragma: no cover
    run(main())
