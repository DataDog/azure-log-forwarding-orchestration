#!/usr/bin/env python

# usage: uninstall.py [-h] [-d] [-s SUBSCRIPTION] [-y]
#
# Uninstall Datadog Log Forwarding Orchestration from an Azure environment
#
# optional arguments:
#   -h, --help            show this help message and exit
#   -d, --dry-run         Run the script in dry-run mode. No changes will be made to the Azure environment
#   -s SUBSCRIPTION, --subscription SUBSCRIPTION
#                         Specify subscription ID to uninstall artifacts from. If not provided, all subscriptions will be searched
#   -y, --yes             Skip all user prompts. This will delete all detected installations without confirmation

import argparse
import json
import subprocess
from collections import defaultdict
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from itertools import cycle
from logging import INFO, WARNING, basicConfig, getLogger
from re import search
from time import sleep
from typing import Any, Final

getLogger("azure").setLevel(WARNING)
log = getLogger("uninstaller")

# ===== User settings ===== #
DRY_RUN_SETTING = False
SKIP_PROMPTS_SETTING = False

# ===== Constants ===== #
CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX: Final = "lfostorage"
DIAGNOSTIC_SETTING_PREFIX: Final = "datadog_log_forwarding_"
SEPARATOR: Final = "\n==============================\n"
ALLOWED_TYPES_PER_PROVIDER: Final = {
    "Microsoft.AAD": {"DomainServices"},
    "Microsoft.AgFoodPlatform": {"farmBeats"},
    "Microsoft.AnalysisServices": {"servers"},
    "Microsoft.ApiManagement": {"service"},
    "Microsoft.App": {"managedEnvironments"},
    "Microsoft.AppConfiguration": {"configurationStores"},
    "Microsoft.AppPlatform": {"spring"},
    "Microsoft.Attestation": {"attestationProviders"},
    "Microsoft.Automation": {"automationAccounts"},
    "Microsoft.AutonomousDevelopmentPlatform": {"accounts", "workspaces"},
    "microsoft.avs": {"privateClouds"},
    "Microsoft.AzureDataTransfer": {"connections/flows"},
    "microsoft.azureplaywrightservice": {"accounts"},
    "microsoft.azuresphere": {"catalogs"},
    "Microsoft.Batch": {"batchaccounts"},
    "microsoft.botservice": {"botservices"},
    "Microsoft.Cache": {"redis", "redisEnterprise/databases"},
    "Microsoft.Cdn": {
        "cdnwebapplicationfirewallpolicies",
        "profiles",
        "profiles/endpoints",
    },
    "Microsoft.Chaos": {"experiments"},
    "Microsoft.ClassicNetwork": {"networksecuritygroups"},
    "Microsoft.CodeSigning": {"codesigningaccounts"},
    "Microsoft.CognitiveServices": {"accounts"},
    "Microsoft.Communication": {"CommunicationServices"},
    "microsoft.community": {"communityTrainings"},
    "Microsoft.Compute": {"virtualMachines"},
    "Microsoft.ConfidentialLedger": {"ManagedCCF", "ManagedCCFs"},
    "Microsoft.ConnectedCache": {
        "CacheNodes",
        "enterpriseMccCustomers",
        "ispCustomers",
    },
    "Microsoft.ConnectedVehicle": {"platformAccounts"},
    "Microsoft.ContainerInstance": {"containerGroups"},
    "Microsoft.ContainerRegistry": {"registries"},
    "Microsoft.ContainerService": {"fleets", "managedClusters"},
    "Microsoft.CustomProviders": {"resourceproviders"},
    "Microsoft.D365CustomerInsights": {"instances"},
    "Microsoft.Dashboard": {"grafana"},
    "Microsoft.Databricks": {"workspaces"},
    "Microsoft.DataFactory": {"factories"},
    "Microsoft.DataLakeAnalytics": {"accounts"},
    "Microsoft.DataLakeStore": {"accounts"},
    "Microsoft.DataProtection": {"BackupVaults"},
    "Microsoft.DataShare": {"accounts"},
    "Microsoft.DBforMariaDB": {"servers"},
    "Microsoft.DBforMySQL": {"flexibleServers", "servers"},
    "Microsoft.DBforPostgreSQL": {"flexibleServers", "servers", "serversv2"},
    "Microsoft.DBForPostgreSQL": {"serverGroupsv2"},
    "Microsoft.DesktopVirtualization": {
        "appAttachPackages",
        "applicationgroups",
        "hostpools",
        "scalingplans",
        "workspaces",
    },
    "Microsoft.DevCenter": {"devcenters"},
    "Microsoft.Devices": {"IotHubs", "provisioningServices"},
    "Microsoft.DevOpsInfrastructure": {"pools"},
    "Microsoft.DigitalTwins": {"digitalTwinsInstances"},
    "Microsoft.DocumentDB": {"cassandraClusters", "DatabaseAccounts", "mongoClusters"},
    "Microsoft.EventGrid": {
        "domains",
        "namespaces",
        "partnerNamespaces",
        "partnerTopics",
        "systemTopics",
        "topics",
    },
    "Microsoft.EventHub": {"Namespaces"},
    "Microsoft.HardwareSecurityModules": {"cloudHsmClusters"},
    "Microsoft.HealthcareApis": {
        "services",
        "workspaces/dicomservices",
        "workspaces/fhirservices",
        "workspaces/iotconnectors",
    },
    "Microsoft.HealthDataAIServices": {"deidServices"},
    "microsoft.insights": {"autoscalesettings", "components"},
    "Microsoft.Insights": {"datacollectionrules"},
    "microsoft.keyvault": {"managedhsms"},
    "Microsoft.KeyVault": {"vaults"},
    "microsoft.kubernetes": {"connectedClusters"},
    "Microsoft.Kusto": {"clusters"},
    "microsoft.loadtestservice": {"loadtests"},
    "Microsoft.Logic": {
        "IntegrationAccounts",
        "Workflows",
    },
    "Microsoft.MachineLearningServices": {
        "",
        "registries",
        "workspaces",
        "workspaces/onlineEndpoints",
    },
    "Microsoft.ManagedNetworkFabric": {"networkDevices"},
    "Microsoft.Media": {
        "mediaservices",
        "mediaservices/liveEvents",
        "mediaservices/streamingEndpoints",
        "videoanalyzers",
    },
    "Microsoft.Monitor": {"accounts"},
    "Microsoft.NetApp": {
        "netAppAccounts/capacityPools",
        "netAppAccounts/capacityPools/volumes",
    },
    "Microsoft.Network": {
        "applicationgateways",
        "azureFirewalls",
        "dnsResolverPolicies",
        "expressRouteCircuits",
        "frontdoors",
        "loadBalancers",
        "networkManagers",
        "networkManagers/ipamPools",
        "networksecuritygroups",
        "networkSecurityPerimeters",
        "networkSecurityPerimeters/profiles",
        "publicIPAddresses",
        "publicIPPrefixes",
        "trafficManagerProfiles",
        "virtualNetworks",
    },
    "microsoft.network": {
        "bastionHosts",
        "p2svpngateways",
        "virtualnetworkgateways",
        "vpngateways",
    },
    "Microsoft.NetworkAnalytics": {"DataProducts"},
    "Microsoft.NetworkCloud": {
        "bareMetalMachines",
        "clusterManagers",
        "clusters",
        "storageAppliances",
    },
    "Microsoft.NetworkFunction": {"azureTrafficCollectors"},
    "Microsoft.NotificationHubs": {"namespaces", "namespaces/notificationHubs"},
    "MICROSOFT.OPENENERGYPLATFORM": {"ENERGYSERVICES"},
    "Microsoft.OpenLogisticsPlatform": {"Workspaces"},
    "Microsoft.OperationalInsights": {"workspaces"},
    "Microsoft.PlayFab": {"titles"},
    "Microsoft.PowerBI": {"tenants", "tenants/workspaces"},
    "Microsoft.PowerBIDedicated": {"capacities"},
    "Microsoft.ProviderHub": {"providerMonitorSettings", "providerRegistrations"},
    "microsoft.purview": {"accounts"},
    "Microsoft.RecoveryServices": {"Vaults"},
    "Microsoft.Relay": {"namespaces"},
    "Microsoft.Search": {"searchServices"},
    "Microsoft.Security": {"antiMalwareSettings", "defenderForStorageSettings"},
    "Microsoft.ServiceBus": {"Namespaces"},
    "Microsoft.ServiceNetworking": {"trafficControllers"},
    "Microsoft.SignalRService": {
        "SignalR",
        "SignalR/replicas",
        "WebPubSub",
        "WebPubSub/replicas",
    },
    "microsoft.singularity": {"accounts"},
    "Microsoft.Sql": {
        "managedInstances",
        "managedInstances/databases",
        "servers/databases",
    },
    "Microsoft.Storage": {
        "storageAccounts/blobServices",
        "storageAccounts/fileServices",
        "storageAccounts/queueServices",
        "storageAccounts/tableServices",
    },
    "Microsoft.StorageCache": {"amlFilesystems", "caches"},
    "Microsoft.StorageMover": {"storageMovers"},
    "Microsoft.StreamAnalytics": {"streamingjobs"},
    "Microsoft.Synapse": {
        "workspaces",
        "workspaces/bigDataPools",
        "workspaces/kustoPools",
        "workspaces/scopePools",
        "workspaces/sqlPools",
    },
    "microsoft.videoindexer": {"accounts"},
    "Microsoft.Web": {"hostingEnvironments", "sites", "sites/slots", "staticsites"},
    "microsoft.workloads": {"sapvirtualinstances"},
    "NGINX.NGINXPLUS": {"nginxDeployment"},
}
ALLOWED_RESOURCE_TYPES: Final = [
    f"{rp}/{rt}".casefold() for rp, resource_types in ALLOWED_TYPES_PER_PROVIDER.items() for rt in resource_types
]
ALLOWED_RESOURCE_TYPES_FILTER: Final = " || ".join([f"type == '{rt}'" for rt in ALLOWED_RESOURCE_TYPES])
ALLOWED_RESOURCE_TYPES_FILTER_GRAPH: Final = " or ".join([f"type =~ '{rt}'" for rt in ALLOWED_RESOURCE_TYPES])
PROGRESS_BAR_LENGTH: Final = 40
MAX_RETRIES = 6
RESOURCE_GROUP_DELETION_POLLING_DELAY = 5  # seconds
THREAD_POOL_SIZE = 100  # https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/request-limits-and-throttling#migrating-to-regional-throttling-and-token-bucket-algorithm

# ===== Errors ===== #
REFRESH_TOKEN_EXPIRED_ERROR = "AADSTS700082"
AZURE_THROTTLING_ERROR = "TooManyRequests"
AUTHORIZATION_ERROR = "AuthorizationFailed"


# ===== Utility ===== #
def first_key_of(d: dict[str, Any]) -> str:
    if not d:
        raise ValueError("Empty dictionary")
    return next(iter(d))


def space_separated(iter: Iterable) -> str:
    return " ".join(iter)


def indented_log_of(iter: Iterable) -> str:
    formatted = "\n".join(f"\t- {item}" for item in iter)
    return f"\n{formatted}\n"


def formatted_number(n: int) -> str:
    return f"{n:,}"


def dry_run_of(s: str) -> str:
    msg = s[0].lower() + s[1:]
    return f"DRY RUN | Would be {msg}"


def print_progress(current: int, total: int):
    if total == 0:
        return

    progress = current / total
    done_bar = int(PROGRESS_BAR_LENGTH * progress)
    leftover_bar = PROGRESS_BAR_LENGTH - done_bar
    percent_done = f"{progress * 100:.0f}%"
    is_done = current == total
    print(
        f"[{'#' * done_bar + '-' * (leftover_bar)}] {current}/{total} ({percent_done})",
        end="\tDone!\n" if is_done else "\r",
        flush=True,
    )


SPINNER = cycle(["—", "\\", "|", "/"])


def progress_spinner(total: int, current: Callable[[], int]):
    if total == 0:
        return
    while (c := current()) != total:
        progress = c / total
        done_bar = int(PROGRESS_BAR_LENGTH * progress)
        leftover_bar = PROGRESS_BAR_LENGTH - done_bar
        print(
            f" {next(SPINNER)} [{'#' * done_bar + '-' * (leftover_bar)}] {c}/{total} ({progress * 100:.0f}%)",
            end="\r",
            flush=True,
        )
        sleep(0.2)
    print(f"   [{'#' * PROGRESS_BAR_LENGTH}] {total}/{total} (100%) Done!")


class AuthError(Exception):
    pass


class RefreshTokenError(Exception):
    pass


def try_regex_access_error(cmd: str, stderr: str):
    # Sample:
    # (AuthorizationFailed) The client 'user@example.com' with object id '00000000-0000-0000-0000-000000000000'
    # does not have authorization to perform action 'Microsoft.Storage/storageAccounts/read'
    # over scope '/subscriptions/00000000-0000-0000-0000-000000000000' or the scope is invalid.
    # If access was recently granted, please refresh your credentials.

    client_match = search(r"client '([^']*)'", stderr)
    action_match = search(r"action '([^']*)'", stderr)
    scope_match = search(r"scope '([^']*)'", stderr)

    if action_match and scope_match and client_match:
        client = client_match.group(1)
        action = action_match.group(1)
        scope = scope_match.group(1)
        raise AuthError(f"Insufficient permissions for {client} to perform {action} on {scope}")


# ===== Artifact Deletion Summaries ===== #
def role_assignment_summary(role_assignments: list[Any]) -> str:
    summary = f"\tRole Assignments: {'None' if not role_assignments else ''}\n"

    for role_assignment in role_assignments:
        definition_name = role_assignment["roleDefinitionName"]
        principal_id = role_assignment["principalId"]
        summary += f"\t\t- {definition_name} for principal {principal_id}\n"

    return summary


def diagnostic_setting_summary(resource_ds_map: dict[str, list[str]]) -> str:
    ds_resource_count = defaultdict(int)
    for _, ds_list in resource_ds_map.items():
        for ds_name in ds_list:
            ds_resource_count[ds_name] += 1

    summary = f"\tDiagnostic Settings: {'None' if not ds_resource_count else ''}\n"
    for ds_name, resource_count in ds_resource_count.items():
        summary += f"\t\t- {ds_name} for {resource_count} resources\n"

    return summary


def resource_group_summary(resource_groups: list[str]) -> str:
    summary = f"\tResource Groups: {'None' if not resource_groups else ''}\n"
    for rg in resource_groups:
        summary += f"\t\t- {rg}\n"

    return summary


def uninstall_summary(
    sub_to_rg_deletions: dict[str, list[str]],
    sub_id_to_name: dict[str, str],
    role_assignment_deletions: dict[str, list[Any]],
    sub_diagnostic_setting_deletions: dict[str, dict[str, list[str]]],
) -> str:
    header = "Deleting the following artifacts"
    deletion_summary = f"{SEPARATOR}{dry_run_of(header) if DRY_RUN_SETTING else header}:\n"

    for sub_id, rg_list in sub_to_rg_deletions.items():
        deletion_summary += f"From subscription {sub_id_to_name[sub_id]} ({sub_id}):\n"

        role_assignments = role_assignment_deletions[sub_id]
        deletion_summary += role_assignment_summary(role_assignments)

        resource_ds_map = sub_diagnostic_setting_deletions[sub_id]
        deletion_summary += diagnostic_setting_summary(resource_ds_map)

        deletion_summary += resource_group_summary(rg_list)

    return deletion_summary


# ===== Azure Commands ===== #
def az(cmd: str) -> str:
    """Runs az command with exponential backoff/retry, returns stdout"""

    delay = 2  # seconds
    az_cmd = f"az {cmd}"

    for attempt in range(MAX_RETRIES):
        try:
            result = subprocess.run(az_cmd, shell=True, check=True, text=True, capture_output=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            stderr = str(e.stderr)
            resource_group_deletion_complete = (
                "ResourceGroupNotFound" in stderr
                and "resource list" in cmd
                and "--resource-group" in cmd
                and '--query "length([])"' in cmd
            )
            if resource_group_deletion_complete:
                return "0"  # handle expected error for polling RG deletion progress - return 0 to indicate there's no resources left
            if AZURE_THROTTLING_ERROR in stderr:
                if attempt < MAX_RETRIES - 1:
                    log.warning(f"Azure throttling ongoing. Retrying in {delay} seconds...")
                    sleep(delay)
                    delay *= 2
                    continue

                log.error("Rate limit exceeded. Please wait a few minutes and try again.")
                raise SystemExit(1) from None
            if REFRESH_TOKEN_EXPIRED_ERROR in stderr:
                raise RefreshTokenError(f"Auth token is expired. Refresh token before running '{az_cmd}'") from None
            if AUTHORIZATION_ERROR in stderr:
                try_regex_access_error(cmd, stderr)
                raise AuthError(f"Insufficient permissions to access resource when executing '{az_cmd}'") from None

            log.error(f"Error running Azure command:\n{e.stderr}")
            raise

    raise SystemExit(1)  # unreachable


def set_extension_config():
    """Sets up the Azure CLI to auto-install extensions if they're required for certain commands.
    We use Azure Resource Graph to query resource information, so we need the resource graph extension"""
    az("config set extension.use_dynamic_install=yes_without_prompt")


def list_users_subscriptions(sub_id=None) -> dict[str, str]:
    """Returns a mapping of subscription ID to subscription name for all subscriptions accessible by the current user"""
    if sub_id is None:
        log.info("Fetching details for all subscriptions accessible by current user... ")
        print_progress(0, 1)
        subs_json = json.loads(az("account list --output json"))
        print_progress(1, 1)
        print(f"Found {len(subs_json)} subscription(s)")
        return {sub["id"]: sub["name"] for sub in subs_json}

    log.info(f"Fetching details for subscription {sub_id}... ")
    subs_json = None
    print_progress(0, 1)

    try:
        subs_json = json.loads(az(f"account show --name {sub_id} --output json"))
    except subprocess.CalledProcessError as e:
        print_progress(1, 1)
        if f"Subscription '{sub_id}' not found" in str(e.stderr):
            log.error(f"Subscription '{sub_id}' not found, exiting")

        log.error(f"Error fetching subscription details: {e.stderr}")
        raise SystemExit(1) from e

    print_progress(1, 1)
    print(f"Found {subs_json['name']} ({subs_json['id']})")
    return {subs_json["id"]: subs_json["name"]}


def list_resources_graph(sub_id: str, sub_name: str) -> set[str]:
    """Returns a set of resource IDs for all resources that LFO could process in a given subscription"""
    log.info(f"Searching for resources in {sub_name} ({sub_id})... ")

    resource_ids = set()
    result = json.loads(
        az(
            f"graph query -q \"Resources | where subscriptionId =~ '{sub_id}' | where {ALLOWED_RESOURCE_TYPES_FILTER_GRAPH} | summarize by id\" --first 1000 --output json"
        )
    )

    resource_ids.update([r["id"] for r in result["data"]])

    # The max number of resources returned is 1000. If there are more
    # than 1000 resources, use skip token to paginate and get the next batch
    skip_token = result["skip_token"]
    while skip_token:
        result = json.loads(
            az(
                f'graph query --skip-token {skip_token} -q "Resources | where {ALLOWED_RESOURCE_TYPES_FILTER_GRAPH} | project id" --first 1000 --output json'
            )
        )
        skip_token = result["skip_token"]
        resource_ids.update([r["id"] for r in result["data"]])

    return resource_ids


def list_resources(sub_id: str, sub_name: str) -> set[str]:
    """Returns a set of resource IDs for all resources that LFO would process in a given subscription"""
    log.info(f"Searching for resources in {sub_name} ({sub_id})... ")

    resource_ids = json.loads(
        az(f'resource list --subscription {sub_id} --query "[?{ALLOWED_RESOURCE_TYPES_FILTER}].id" --output json')
    )
    print(f"Found {formatted_number(len(resource_ids))} resource(s)")

    return set(resource_ids)


def num_resources_in_group(sub_id: str, resource_group: str) -> int:
    """Returns the number of resources in a given resource group. If the resource group is not found, returns 0"""
    try:
        return int(
            az(
                f'resource list --subscription {sub_id} --resource-group {resource_group} --query "length([])" --output tsv'
            )
        )
    except subprocess.CalledProcessError as e:
        if "ResourceGroupNotFound" in str(e.stderr):
            return 0
        raise e


def find_sub_control_planes(sub_id: str, sub_name: str) -> dict[str, str]:
    """Queries for LFO control planes in single subscription, returns mapping of resource group name to control plane storage account name"""

    log.debug("Searching for Datadog log forwarding instance in subscription '%s' (%s)... ", sub_name, sub_id)
    cmd = f"storage account list --subscription {sub_id} --query \"[?starts_with(name,'{CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX}')].{{resourceGroup:resourceGroup, name:name}}\" --output json"

    try:
        storage_accounts_json = json.loads(az(cmd))
        lfo_install_map = {account["resourceGroup"]: account["name"] for account in storage_accounts_json}
        log.debug("Found %s log forwarding instance(s)", len(lfo_install_map))
        return lfo_install_map
    except RefreshTokenError as e:
        log.warning(
            f"Ran into authentication token error searching {sub_name} ({sub_id}) - excluding it from search results. Refresh your credentials if necessary."
        )
        log.debug("Authentication Error details for %s (%s)", sub_name, sub_id, exc_info=e)
        return {}
    except AuthError as e:
        log.warning(f"Ran into authorization error searching {sub_name} ({sub_id}) - excluding it from search results.")
        log.debug("Authorization Error details for %s (%s)", sub_name, sub_id, exc_info=e)
        return {}


def find_all_control_planes(
    sub_id_to_name: dict[str, str],
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """
    Queries for all LFO control planes that the user has access to.
    Returns 2 dictionaries - a subcription ID to resource group mapping for the subscriptions we can access and a resource group to control plane ID mapping
    """

    with ThreadPoolExecutor(THREAD_POOL_SIZE) as tpe:
        futures = [tpe.submit(find_sub_control_planes, sub_id, sub_name) for sub_id, sub_name in sub_id_to_name.items()]
        progress_spinner(len(futures), lambda: sum(not f.running() for f in futures))

    sub_to_rg = defaultdict(list)
    rg_to_lfo_id = defaultdict(list)
    for control_planes_future, sub_id in zip(futures, sub_id_to_name):
        if e := control_planes_future.exception():
            log.error(f"Unexpected error searching for control planes in {sub_id_to_name[sub_id]} ({sub_id}): {e}")
            continue

        control_planes = control_planes_future.result()

        for resource_group_name, storage_account_name in control_planes.items():
            sub_to_rg[sub_id].append(resource_group_name)
            rg_to_lfo_id[resource_group_name].append(
                storage_account_name.removeprefix(CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX)
            )

    return sub_to_rg, rg_to_lfo_id


def sub_has_rg(sub_id: str, rg_name: str) -> bool:
    """Returns True if the given subscription has the given resource group, False otherwise"""
    try:
        return az(f"group exists --name {rg_name} --subscription {sub_id}").strip().casefold() == "true"
    except subprocess.CalledProcessError:
        return False


def find_role_assignments(sub_id: str, sub_name: str, control_plane_ids: set) -> list[dict[str, str]]:
    """Returns JSON array of role assignments (properties = id, roleDefinitionName, principalId)"""

    log.info(f"Looking for Datadog role assignments in {sub_name} ({sub_id})... ")

    description_filter = " || ".join(f"description == 'ddlfo{id}'" for id in control_plane_ids)
    role_assignment_json = json.loads(
        az(
            f'role assignment list --all --subscription {sub_id} --query "[?description != null && ({description_filter})].{{id: id, roleDefinitionName: roleDefinitionName, principalId: principalId}}" --output json'
        )
    )

    print(f"Found {len(role_assignment_json)} role assignment(s)")

    return role_assignment_json


def delete_role_assignments(sub_id: str, role_assigments_json: list[dict[str, str]]):
    if DRY_RUN_SETTING:
        return

    ids = {role["id"] for role in role_assigments_json}
    if not ids:
        log.info(f"Did not find any role assignments to delete in subscription {sub_id}")
        return

    # --include-inherited flag will also try to delete the role assignment from the management group scope if possible
    az(f"role assignment delete --ids {space_separated(ids)} --subscription {sub_id} --include-inherited --yes")


def find_diagnostic_settings(sub_id: str, sub_name: str, control_plane_ids: set) -> dict[str, list[str]]:
    """Returns mapping of resource ID to list of LFO diagnostic settings"""

    resource_ids = list_resources_graph(sub_id, sub_name)
    resource_count = len(resource_ids)
    resource_ds_map = defaultdict(list)
    if not resource_ids:
        return resource_ds_map

    # log.info(f"Searching for Datadog log forwarding diagnostic settings in {sub_name} ({sub_id})... ")
    # ds_count = 0
    diagnostic_settings_filter = " || ".join(f"name == '{DIAGNOSTIC_SETTING_PREFIX}{id}'" for id in control_plane_ids)
    # with ThreadPoolExecutor(THREAD_POOL_SIZE) as tpe:
    #     ds_futures = [
    #         tpe.submit(
    #             az,
    #             f'monitor diagnostic-settings list --resource {resource_id} --query "[?{diagnostic_settings_filter}].name"',
    #         )
    #         for resource_id in resource_ids
    #     ]
    #     progress_spinner(resource_count, lambda: sum(f.done() for f in ds_futures))

    # for resource_id, ds_future in zip(resource_ids, ds_futures):
    #     ds_names = json.loads(ds_future.result())
    #     for ds_name in ds_names:
    #         resource_ds_map[resource_id].append(ds_name)
    #         ds_count += 1

    # Process in batches
    BATCH_SIZE = 25
    total_processed = 0
    ds_count = 0
    batch_count = 0
    for i in range(0, resource_count, BATCH_SIZE):
        batch_count += 1
        log.info(f"Starting batch {batch_count} of {(resource_count + BATCH_SIZE - 1) // BATCH_SIZE}")
        batch = list(resource_ids)[i : i + BATCH_SIZE]
        batch_size = len(batch)

        try:
            log.info(f"Creating thread pool with {min(THREAD_POOL_SIZE, batch_size)} threads")
            with ThreadPoolExecutor(min(THREAD_POOL_SIZE, batch_size)) as tpe:
                log.info(f"Submitting {batch_size} tasks to thread pool")
                ds_futures = []
                for resource_id in batch:
                    future = tpe.submit(
                        az,
                        f'monitor diagnostic-settings list --resource {resource_id} --query "[?{diagnostic_settings_filter}].name"',
                    )
                    ds_futures.append(future)
                # Show progress within this batch
                # local_spinner = lambda: sum(not f.running() for f in ds_futures)
                # progress_spinner(batch_size, local_spinner)
                log.info(f"Submitted {len(ds_futures)} tasks. Waiting for completion...")

            log.info(f"Thread pool for batch {batch_count} has completed")

            # Process results after ThreadPoolExecutor is closed
            for idx, (resource_id, ds_future) in enumerate(zip(batch, ds_futures)):
                log.info(f"Processing result {idx + 1}/{batch_size} for batch {batch_count}")
                log.info(f"Fetching diagnostic settings for {resource_id}")
                try:
                    ds_names = json.loads(ds_future.result())
                    for ds_name in ds_names:
                        resource_ds_map[resource_id].append(ds_name)
                        ds_count += 1
                except Exception as e:
                    log.warning(f"Error getting diagnostic settings for {resource_id}: {e}")

            total_processed += batch_size
            log.info(
                f"Processed {total_processed}/{resource_count} resources ({(total_processed / resource_count) * 100:.1f}%)"
            )
        except Exception as e:
            log.error(f"Error processing batch {batch_count}: {e}", exc_info=True)

    log.info(f"Found {ds_count} diagnostic settings to remove (searched through {resource_count} resources)")
    return resource_ds_map


def delete_diagnostic_setting(sub_id: str, resource_id: str, ds_name: str):
    if DRY_RUN_SETTING:
        return

    az(f"monitor diagnostic-settings delete --name {ds_name} --resource {resource_id} --subscription {sub_id}")


def delete_roles_diag_settings(
    sub_id: str,
    sub_role_assignment_deletions: dict[str, list[dict[str, str]]],
    sub_diagnostic_setting_deletions: dict[str, dict[str, list[str]]],
):
    delete_log = "Deleting role assignments and diagnostic settings"
    log.info(dry_run_of(delete_log) if DRY_RUN_SETTING else delete_log)

    with ThreadPoolExecutor(THREAD_POOL_SIZE) as tpe:
        futures: list[Future[None]] = []
        for sub_id, role_assignments_json in sub_role_assignment_deletions.items():
            if role_assignments_json:
                futures.append(tpe.submit(delete_role_assignments, sub_id, role_assignments_json))
        for sub_id, resource_ds_map in sub_diagnostic_setting_deletions.items():
            for resource_id, ds_names in resource_ds_map.items():
                for ds_name in ds_names:
                    futures.append(tpe.submit(delete_diagnostic_setting, sub_id, resource_id, ds_name))

        progress_spinner(len(futures), lambda: sum(not f.running() for f in futures))


def start_resource_group_delete(sub_id: str, resource_group_list: list[str]):
    for resource_group in resource_group_list:
        rg_deletion_log = (
            f"Starting resource group '{resource_group}' deletion in background since it can take some time... "
        )
        if DRY_RUN_SETTING:
            log.info(dry_run_of(rg_deletion_log))
            return

        log.info(rg_deletion_log)

        # --no-wait will initiate delete and immediately return
        az(f"group delete --subscription {sub_id} --name {resource_group} --yes --no-wait")


def wait_for_resource_group_deletion(sub_to_rg_deletions: dict[str, list[str]]):
    log_msg = "Checking resource group deletion status (this can take 15+ minutes depending on number of resources in the group)... "
    log.info(f"{dry_run_of(log_msg) if DRY_RUN_SETTING else log_msg}")

    if DRY_RUN_SETTING:
        return

    total_resource_count = 0
    for sub_id, rg_list in sub_to_rg_deletions.items():
        for rg in rg_list:
            total_resource_count += num_resources_in_group(sub_id, rg)

    num_resources = [total_resource_count]
    last_check = [datetime.now()]

    def current_resources_left():
        if (datetime.now() - last_check[0]).total_seconds() > RESOURCE_GROUP_DELETION_POLLING_DELAY:
            last_check[0] = datetime.now()
            with ThreadPoolExecutor(THREAD_POOL_SIZE) as tpe:
                futures = [
                    tpe.submit(num_resources_in_group, sub_id, rg)
                    for sub_id, rg_list in sub_to_rg_deletions.items()
                    for rg in rg_list
                ]
            num_resources[0] = sum(f.result() for f in futures)
        return total_resource_count - num_resources[0]

    progress_spinner(total_resource_count, current_resources_left)


# ===== User Interaction =====  #
def confirm_uninstall(
    sub_to_rg_deletions: dict[str, list[str]],
    sub_id_to_name: dict[str, str],
    role_assignment_deletions: dict[str, list[dict[str, str]]],
    sub_diagnostic_setting_deletions: dict[str, dict[str, list[str]]],
):
    """Displays summary of what will be deleted and prompts user for confirmation. Returns true if user confirms, false otherwise"""
    summary = uninstall_summary(
        sub_to_rg_deletions,
        sub_id_to_name,
        role_assignment_deletions,
        sub_diagnostic_setting_deletions,
    )
    log.warning(summary)

    if SKIP_PROMPTS_SETTING:
        return True

    choice = input("Continue? (y/n): ").lower().strip()
    while choice not in ["y", "n"]:
        choice = input("Continue? (y/n): ").lower().strip()

    if choice == "n":
        log.info("Exiting.")
        raise SystemExit(0)


def choose_resource_groups_to_delete(resource_groups_in_sub: list[str]) -> list[str]:
    """Given list of resource groups, prompt the user to select what to delete. Returns what was selected"""
    if SKIP_PROMPTS_SETTING:
        return resource_groups_in_sub

    prompt = """
    Enter the resource group name you would like to remove
    - To remove all of them, enter '*'
    - To remove nothing, enter '-'
    : """
    chosen_rg = input(prompt).strip().lower()

    while chosen_rg not in resource_groups_in_sub:
        if chosen_rg == "*":
            return resource_groups_in_sub
        if chosen_rg == "-":
            return []

        chosen_rg = (
            input("Please enter a valid resource group name from the list above, '*', or '-' \n: ").strip().lower()
        )

    return [chosen_rg]


def identify_resource_groups_to_delete(sub_id: str, sub_name: str, resource_groups_in_sub: list[str]) -> list[str]:
    """For given subscription, prompt the user to choose which resource group (or all) to delete if there's multiple. Returns a list of resource groups to delete"""

    if len(resource_groups_in_sub) == 1:
        log.info(
            f"Found single resource group with log forwarding artifact in '{sub_name}' ({sub_id}): '{resource_groups_in_sub[0]}'"
        )
        return resource_groups_in_sub

    log.info(
        f"Found multiple resource groups with log forwarding artifacts in '{sub_name}' ({sub_id}): {indented_log_of(resource_groups_in_sub)}"
    )

    return choose_resource_groups_to_delete(resource_groups_in_sub)


def mark_rg_deletions_per_sub(
    sub_id_to_name: dict[str, str], sub_id_to_rgs: dict[str, list[str]]
) -> dict[str, list[str]]:
    """Returns mapping of subscription ID to resource groups within it to delete. May prompt user for input if multiple resource groups are found in a sub"""

    sub_id_to_rg_deletions = {}
    if not sub_id_to_rgs:
        log.info("Did not find any Datadog log forwarding installations")
        return sub_id_to_rg_deletions

    if len(sub_id_to_rgs) == 1:
        sub_id = first_key_of(sub_id_to_rgs)
        sub_name = sub_id_to_name[sub_id]
        resource_groups_in_sub = sub_id_to_rgs[sub_id]
        rgs_to_delete = identify_resource_groups_to_delete(sub_id, sub_name, resource_groups_in_sub)
        if any(rgs_to_delete):
            sub_id_to_rg_deletions[sub_id] = rgs_to_delete
        return sub_id_to_rg_deletions

    log.info("Found log forwarding installations in multiple subscriptions")
    for sub_id, rg_list in sub_id_to_rgs.items():
        sub_name = sub_id_to_name[sub_id]
        rgs_to_delete = identify_resource_groups_to_delete(sub_id, sub_name, rg_list)
        if any(rgs_to_delete):
            sub_id_to_rg_deletions.setdefault(sub_id, []).extend(rgs_to_delete)

        # Check if other subs have the resource group that the user chose to delete, mark them for deletion if so
        for sub_id, _ in sub_id_to_rgs.items():
            for rg in rgs_to_delete:
                if sub_has_rg(sub_id, rg) and rg not in sub_id_to_rg_deletions.get(sub_id, []):
                    sub_id_to_rg_deletions.setdefault(sub_id, []).append(rg)
    return sub_id_to_rg_deletions


def mark_control_plane_deletions(
    sub_to_rg_deletions: dict[str, list[str]], rg_to_lfo_id: dict[str, list[str]]
) -> set[str]:
    """Based on the resource groups the user selected previously, return the control plane IDs to target for deletion"""

    return {
        lfo_id for rg_deletions in sub_to_rg_deletions.values() for rg in rg_deletions for lfo_id in rg_to_lfo_id[rg]
    }


def mark_role_assignment_deletions(
    sub_to_rg_deletions: dict[str, list[str]],
    sub_id_to_name: dict,
    control_plane_id_deletions: set[str],
) -> dict[str, list[dict[str, str]]]:
    role_assignment_deletions = defaultdict(list)
    for sub_id in sub_to_rg_deletions:
        sub_name = sub_id_to_name[sub_id]
        role_assignment_json = find_role_assignments(sub_id, sub_name, control_plane_id_deletions)
        if role_assignment_json:
            role_assignment_deletions[sub_id] = role_assignment_json

    return role_assignment_deletions


def mark_diagnostic_setting_deletions(
    sub_to_rg_deletions: dict[str, list[str]],
    sub_id_to_name: dict,
    control_plane_id_deletions: set[str],
) -> dict[str, dict[str, list[str]]]:
    sub_diagnostic_setting_deletions = defaultdict(dict)
    for sub_id in sub_to_rg_deletions:
        sub_name = sub_id_to_name[sub_id]
        resource_ds_map = find_diagnostic_settings(sub_id, sub_name, control_plane_id_deletions)
        if resource_ds_map:
            sub_diagnostic_setting_deletions[sub_id] = resource_ds_map

    return sub_diagnostic_setting_deletions


def parse_args():
    parser = argparse.ArgumentParser(
        description="Uninstall Datadog Azure Log Forwarding Orchestration from an Azure environment"
    )
    parser.add_argument(
        "-d",
        "--dry-run",
        action="store_true",
        help="Run the script in dry-run mode. No changes will be made to the Azure environment",
    )
    parser.add_argument(
        "-s",
        "--subscription",
        type=str,
        help="Specify subscription ID to uninstall artifacts from. If not provided, all subscriptions will be searched",
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Skip all user prompts. This will delete all detected installations without confirmation",
    )
    args = parser.parse_args()

    global DRY_RUN_SETTING, SKIP_PROMPTS_SETTING
    if args.dry_run:
        DRY_RUN_SETTING = True
        log.info("Dry run enabled, no changes will be made")
    else:
        DRY_RUN_SETTING = False
        log.warning(
            "Deletion of resource groups (& ALL resources within), role assignments, and diagnostic settings will occur as a result of the uninstall process."
        )
        log.warning(
            "If you have created any Azure resources in resource groups managed by Datadog log forwarding, they will be deleted. Perform backups if necessary!"
        )

    if args.yes:
        SKIP_PROMPTS_SETTING = True
        log.warning("Skipping all user prompts. Script will execute without user confirmation")
    return args


def main():
    """
    Overview:
    1) Fetch subscriptions accessible by current user or the single specified one.
    2) For each subscription, search for LFO control planes. If found:
        - Map the subscription to control plane resource group
        - Map the resource group to storage account mapping
    3) For each subscription, determine which LFO resource groups need to be deleted
        - If there is only one resource group, mark it for deletion
        - If there are multiple, user input may be required to disambiguate
    4) Based on the resource groups marked for deletion, note the corresponding control plane IDs
    5) Based on control plane IDs, find corresponding role assignments and diagnostic settings
    6) Display summary of what will be deleted to user. May prompt for confirmation.
    7) Delete role assignments, diagnostic settings, and resource groups.
    8) Confirm the artifacts are deleted successfully.
    """

    args = parse_args()
    set_extension_config()

    sub_id_to_name = list_users_subscriptions(args.subscription)
    sub_id_to_rgs, rg_to_lfo_id = find_all_control_planes(sub_id_to_name)
    sub_to_rg_deletions = mark_rg_deletions_per_sub(sub_id_to_name, sub_id_to_rgs)

    if not any(sub_to_rg_deletions.values()):
        log.info("Could not find any resource groups to delete as part of uninstall process. Exiting.")
        raise SystemExit(0)

    control_plane_id_deletions = mark_control_plane_deletions(sub_to_rg_deletions, rg_to_lfo_id)

    sub_role_assignment_deletions = mark_role_assignment_deletions(
        sub_to_rg_deletions, sub_id_to_name, control_plane_id_deletions
    )
    sub_diagnostic_setting_deletions = mark_diagnostic_setting_deletions(
        sub_to_rg_deletions, sub_id_to_name, control_plane_id_deletions
    )

    confirm_uninstall(
        sub_to_rg_deletions,
        sub_id_to_name,
        sub_role_assignment_deletions,
        sub_diagnostic_setting_deletions,
    )

    for sub_id, rg_list in sub_to_rg_deletions.items():
        sub_name = sub_id_to_name[sub_id]
        log.info(f"{SEPARATOR}Deleting artifacts in {sub_name} ({sub_id})")
        start_resource_group_delete(sub_id, rg_list)
        delete_roles_diag_settings(sub_id, sub_role_assignment_deletions, sub_diagnostic_setting_deletions)

    wait_for_resource_group_deletion(sub_to_rg_deletions)

    log.info("Uninstall done! Exiting.")


if __name__ == "__main__":
    basicConfig(level=INFO)
    main()
