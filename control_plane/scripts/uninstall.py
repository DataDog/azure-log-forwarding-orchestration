#!/usr/bin/env python
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# usage: uninstall.py [-h] [-d] [-s SUBSCRIPTION] [-y] [-cp CONTROL_PLANE]

# Uninstall Datadog Azure Log Forwarding Orchestration from an Azure environment

# options:
#   -h, --help            show this help message and exit
#   -d, --dry-run         Run the script in dry-run mode. No changes will be made to the Azure environment
#   -s SUBSCRIPTION, --subscription SUBSCRIPTION
#                         Specify subscription ID to uninstall artifacts from. If provided, only this subscription will be searched for artifacts
#   -y, --yes             Skip all user prompts. This will delete all detected installations without confirmation
#   -cp CONTROL_PLANE, --control-plane CONTROL_PLANE
#                         Specify control plane ID to search for. If specified, only this control plane ID will be searched for artifacts

import argparse
import concurrent.futures
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
from typing import Any, Final, TypeAlias

getLogger("azure").setLevel(WARNING)
log = getLogger("uninstaller")

# ===== User settings ===== #
DRY_RUN_SETTING = False
SKIP_PROMPTS_SETTING = False
CONTROL_PLANE_ID_SETTING = None
SUBSCRIPTION_ID_SETTING = None

# ===== Constants ===== #
CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX: Final = "lfostorage"
CONTAINER_APP_ENV_RESOURCE_TYPE: Final = "Microsoft.App/managedEnvironments"
DIAGNOSTIC_SETTING_PREFIX: Final = "datadog_log_forwarding_"
FORWARDER_ENVIRONMENT_PREFIX: Final = "dd-log-forwarder-env-"
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
PROGRESS_BAR_LENGTH: Final = 40
MAX_RETRIES = 7
RESOURCE_GROUP_DELETION_POLLING_DELAY = 5  # seconds
MAX_WAIT_TIME = 30  # seconds

# https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/request-limits-and-throttling#migrating-to-regional-throttling-and-token-bucket-algorithm
THREAD_POOL_SIZE = 100
DIAGNOSTIC_SETTING_THREAD_POOL_SIZE = 30  # diagnostic settings require operations per resource - limit threads here because the Azure Cloud Shell VM may OOM

# ===== Errors ===== #
REFRESH_TOKEN_EXPIRED_ERROR = "AADSTS700082"
AZURE_THROTTLING_ERROR = "TooManyRequests"
RESOURCE_COLLECTION_THROTTLING_ERROR = "ResourceCollectionRequestsThrottled"
AUTHORIZATION_ERROR = "AuthorizationFailed"

# ===== Type Aliases ===== #
SubIdToResourceGroupsDict: TypeAlias = dict[str, set[str]]
ResourceGroupToLfoIdsDict: TypeAlias = dict[str, set[str]]
SubIdToRoleAssignmentsDict: TypeAlias = dict[str, list[dict[str, str]]]
SubIdToResourceIdToDiagSettingsDict: TypeAlias = dict[str, dict[str, set[str]]]


# ===== Utility ===== #
def first_key_of(d: dict[str, Any]) -> str:
    if not d:
        raise ValueError("Empty dictionary")
    return next(iter(d))


def space_separated(iter: Iterable[str]) -> str:
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
        is_unknown = role_assignment["principalName"] == ""
        summary += f"\t\t- {definition_name} for principal {principal_id}{' (Unknown)' if is_unknown else ''}\n"

    return summary


def diagnostic_setting_summary(resource_id_to_diag_setting: dict[str, set[str]]) -> str:
    ds_resource_count = defaultdict(int)
    for _, ds_list in resource_id_to_diag_setting.items():
        for ds_name in ds_list:
            ds_resource_count[ds_name] += 1

    summary = f"\tDiagnostic Settings: {'None' if not ds_resource_count else ''}\n"
    for ds_name, resource_count in ds_resource_count.items():
        summary += f"\t\t- {ds_name} for {resource_count} resources\n"

    return summary


def resource_group_summary(resource_groups: set[str]) -> str:
    summary = f"\tResource Groups: {'None' if not resource_groups else ''}\n"
    for rg in resource_groups:
        summary += f"\t\t- {rg}\n"

    return summary


def uninstall_summary(
    sub_id_to_name: dict[str, str],
    sub_to_rg_deletions: SubIdToResourceGroupsDict,
    sub_to_role_assignment_deletions: SubIdToRoleAssignmentsDict,
    sub_to_resource_id_to_diag_setting_deletions: SubIdToResourceIdToDiagSettingsDict,
) -> str:
    if (
        not sub_to_rg_deletions
        and not sub_to_role_assignment_deletions
        and not sub_to_resource_id_to_diag_setting_deletions
    ):
        return "No log forwarding artifacts detected"

    header = "Deleting the following artifacts"
    deletion_summary = f"{SEPARATOR}{dry_run_of(header) if DRY_RUN_SETTING else header}:\n"

    for sub_id, name in sub_id_to_name.items():
        if (
            sub_id in sub_to_rg_deletions
            or sub_id in sub_to_role_assignment_deletions
            or sub_id in sub_to_resource_id_to_diag_setting_deletions
        ):
            deletion_summary += f"From subscription '{name}' ({sub_id}):\n"

            if sub_id in sub_to_role_assignment_deletions:
                role_assignments = sub_to_role_assignment_deletions[sub_id]
                deletion_summary += role_assignment_summary(role_assignments)

            if sub_id in sub_to_resource_id_to_diag_setting_deletions:
                resource_id_to_diag_setting = sub_to_resource_id_to_diag_setting_deletions[sub_id]
                deletion_summary += diagnostic_setting_summary(resource_id_to_diag_setting)

            if sub_id in sub_to_rg_deletions:
                deletion_summary += resource_group_summary(sub_to_rg_deletions[sub_id])

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
            if AZURE_THROTTLING_ERROR in stderr or RESOURCE_COLLECTION_THROTTLING_ERROR in stderr:
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


def list_users_subscriptions() -> dict:
    log.info("Fetching details for all subscriptions accessible by current user... ")
    print_progress(0, 1)
    subs_json = json.loads(az("account list --output json"))
    print_progress(1, 1)
    print(f"Found {len(subs_json)} subscription(s)")
    return {sub["id"]: sub["name"] for sub in subs_json}


def list_resources(sub_id: str, sub_name: str) -> set:
    log.info(f"Searching for resources in {sub_name} ({sub_id})... ")

    resource_ids = json.loads(
        az(f'resource list --subscription {sub_id} --query "[?{ALLOWED_RESOURCE_TYPES_FILTER}].id" --output json')
    )
    print(f"Found {formatted_number(len(resource_ids))} resource(s)")

    return set(resource_ids)


def num_resources_in_group(sub_id: str, resource_group: str) -> int:
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
    """
    Queries for LFO control planes in single subscription.
    Returns mapping of control plane resource group name to control plane storage account name
    """

    if CONTROL_PLANE_ID_SETTING:
        cmd = f"storage account list --subscription {sub_id} --query \"[?name == '{CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX}{CONTROL_PLANE_ID_SETTING}'].{{resourceGroup:resourceGroup, name:name}}\" --output json"
    else:
        cmd = f"storage account list --subscription {sub_id} --query \"[?starts_with(name,'{CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX}')].{{resourceGroup:resourceGroup, name:name}}\" --output json"

    try:
        storage_accounts_json = json.loads(az(cmd))
        control_plane_rg_to_storage_acct_name = {
            account["resourceGroup"]: account["name"] for account in storage_accounts_json
        }
        return control_plane_rg_to_storage_acct_name
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
) -> tuple[SubIdToResourceGroupsDict, ResourceGroupToLfoIdsDict]:
    """
    Queries for all LFO control planes within a set of subscriptions.
    Returns 2 dictionaries:
        - A subscription ID to control plane resource group mapping
        - A control plane resource group to control plane ID mapping
    """

    subs_to_search = filter_subs_to_search(sub_id_to_name, "control planes")

    with ThreadPoolExecutor(THREAD_POOL_SIZE) as tpe:
        futures = [tpe.submit(find_sub_control_planes, sub_id, sub_name) for sub_id, sub_name in subs_to_search.items()]
        progress_spinner(len(futures), lambda: sum(f.done() for f in futures))

    sub_to_control_plane_rg = defaultdict(set)
    control_plane_rg_to_lfo_id = defaultdict(set)
    for control_planes_future, sub_id in zip(futures, subs_to_search):
        if e := control_planes_future.exception():
            log.error(f"Unexpected error searching for control planes in {subs_to_search[sub_id]} ({sub_id}): {e}")
            continue

        try:
            control_plane_rg_to_storage_acct_name = control_planes_future.result(timeout=MAX_WAIT_TIME)
        except concurrent.futures.TimeoutError:
            log.error(f"Timeout searching for control planes in subscription {sub_id}")
            continue

        for resource_group_name, storage_account_name in control_plane_rg_to_storage_acct_name.items():
            sub_to_control_plane_rg[sub_id].add(resource_group_name)
            control_plane_rg_to_lfo_id[resource_group_name].add(
                storage_account_name.removeprefix(CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX)
            )

    return sub_to_control_plane_rg, control_plane_rg_to_lfo_id


def find_all_forwarder_envs(
    sub_id_to_name: dict[str, str],
) -> tuple[SubIdToResourceGroupsDict, SubIdToResourceGroupsDict]:
    """
    Queries for all LFO log forwarders in the user accessible subscriptions.
    Returns 2 dictionaries:
        1) Subcription ID to log forwarder resource group mapping and
        2) Log forwarder resource group to LFO ID mapping
    """

    subs_to_search = filter_subs_to_search(sub_id_to_name, "log forwarders")

    sub_id_to_forwarder_rg: SubIdToResourceGroupsDict = defaultdict(set)
    forwarder_rgs_to_lfo_id: SubIdToResourceGroupsDict = defaultdict(set)
    for sub_id in subs_to_search:
        cmd = f"resource list --subscription {sub_id} --resource-type {CONTAINER_APP_ENV_RESOURCE_TYPE} --query \"[?starts_with(name,'{FORWARDER_ENVIRONMENT_PREFIX}')].{{name:name,resourceGroup:resourceGroup}}\" --output json"
        if CONTROL_PLANE_ID_SETTING:
            cmd = f"resource list --subscription {sub_id} --resource-type {CONTAINER_APP_ENV_RESOURCE_TYPE} --query \"[?starts_with(name,'{FORWARDER_ENVIRONMENT_PREFIX}{CONTROL_PLANE_ID_SETTING}')].{{name:name,resourceGroup:resourceGroup}}\" --output json"

        forwarder_envs = json.loads(az(cmd))
        for forwarder_env in forwarder_envs:
            forwarder_rg = forwarder_env["resourceGroup"]
            forwarder_id = forwarder_env["name"].removeprefix(FORWARDER_ENVIRONMENT_PREFIX).split("-")[0]
            if forwarder_rg not in sub_id_to_forwarder_rg[sub_id]:
                sub_id_to_forwarder_rg[sub_id].add(forwarder_rg)
            if forwarder_id not in forwarder_rgs_to_lfo_id[forwarder_rg]:
                forwarder_rgs_to_lfo_id[forwarder_rg].add(forwarder_id)

    return sub_id_to_forwarder_rg, forwarder_rgs_to_lfo_id


def filter_subs_to_search(sub_id_to_name: dict[str, str], log_context: str) -> dict[str, str]:
    """If the user supplied the subscription ID parameter, try to find it in sub_id_to_name and return it as a single-item dict"""
    if not SUBSCRIPTION_ID_SETTING:
        log.info(f"Searching for Datadog {log_context}... ")
        return sub_id_to_name

    subs_to_search = sub_id_to_name

    if SUBSCRIPTION_ID_SETTING not in sub_id_to_name:
        log.error(f"User-given subscription ID '{SUBSCRIPTION_ID_SETTING}' not found, exiting")
        raise SystemExit(0)

    subs_to_search = {SUBSCRIPTION_ID_SETTING: sub_id_to_name[SUBSCRIPTION_ID_SETTING]}
    log.info(f"Searching for Datadog {log_context} in subscription '{SUBSCRIPTION_ID_SETTING}'")

    return subs_to_search


def sub_has_rg(sub_id: str, rg_name: str) -> bool:
    """Returns True if the given subscription has the given resource group, False otherwise"""
    try:
        return az(f"group exists --name {rg_name} --subscription {sub_id}").strip().casefold() == "true"
    except subprocess.CalledProcessError:
        return False


ROLE_ASSIGNMENT_QUERY_SELECT: Final = (
    "id: id, roleDefinitionName: roleDefinitionName, principalId: principalId, principalName: principalName"
)


def find_role_assignments(sub_id_to_name: dict[str, str], control_plane_ids: set) -> SubIdToRoleAssignmentsDict:
    """Returns JSON array of role assignments"""

    description_filter = " || ".join(f"description == 'ddlfo{id}'" for id in control_plane_ids)
    with ThreadPoolExecutor(THREAD_POOL_SIZE) as tpe:
        futures = [
            tpe.submit(
                az,
                f'role assignment list --all --subscription {sub_id} --query "[?description != null && ({description_filter})].{{{ROLE_ASSIGNMENT_QUERY_SELECT}}}" --output json',
            )
            for sub_id in sub_id_to_name
        ]
        progress_spinner(len(futures), lambda: sum(f.done() for f in futures))

    sub_to_role_assignments_json = {}
    for future, sub_id in zip(futures, sub_id_to_name):
        if future.exception():
            log.error(
                f"Unexpected error searching for role assignments in '{sub_id_to_name[sub_id]}' ({sub_id}): {future.exception()}"
            )
            continue

        try:
            role_assignment = future.result(timeout=MAX_WAIT_TIME)
        except concurrent.futures.TimeoutError:
            log.error(f"Timeout searching for role assignments in subscription {sub_id}")
            continue

        role_assigment_json = json.loads(role_assignment)
        if role_assigment_json:
            log.info(f"Found {len(role_assigment_json)} role assignment(s) in '{sub_id_to_name[sub_id]}' ({sub_id})")
            sub_to_role_assignments_json[sub_id] = role_assigment_json

    return sub_to_role_assignments_json


def find_unknown_role_assignments(sub_id_to_name: dict[str, str]) -> dict[str, list[dict[str, str]]]:
    """Returns a dictionary of subscription ID to list of 'Unknown' role assignments leftover from previous LFO installations."""

    unknown_role_assignments_filter = "[?principalName=='' && description != null && starts_with(description, 'ddlfo')]"

    with ThreadPoolExecutor(THREAD_POOL_SIZE) as tpe:
        futures = [
            tpe.submit(
                az,
                f'role assignment list --all --subscription {sub_id} --include-inherited --include-groups --query "{unknown_role_assignments_filter}.{{{ROLE_ASSIGNMENT_QUERY_SELECT}}}" --output json',
            )
            for sub_id in sub_id_to_name
        ]
        progress_spinner(len(futures), lambda: sum(f.done() for f in futures))

    unknown_role_assignments = {}
    for future, sub_id in zip(futures, sub_id_to_name):
        if future.exception():
            log.error(
                f"Unexpected error searching for 'Unknown' role assignments in '{sub_id_to_name[sub_id]}' ({sub_id}): {future.exception()}"
            )
            continue

        try:
            unknown_role_assignment = future.result(timeout=MAX_WAIT_TIME)
        except concurrent.futures.TimeoutError:
            log.error(f"Timeout searching for 'Unknown' role assignments in subscription {sub_id}")
            continue

        unknown_role_assignment_json = json.loads(unknown_role_assignment)
        if unknown_role_assignment_json:
            log.info(
                f"Found {len(unknown_role_assignment_json)} 'Unknown' role assignment(s) in '{sub_id_to_name[sub_id]}' ({sub_id})"
            )
            unknown_role_assignments[sub_id] = unknown_role_assignment_json

    return unknown_role_assignments


def delete_resource_group(sub_id: str, resource_group: str):
    if DRY_RUN_SETTING:
        return

    # --no-wait will initiate delete and immediately return
    az(f"group delete --subscription {sub_id} --name {resource_group} --yes --no-wait")


def delete_role_assignments(sub_id: str, role_assigments_json: list[dict[str, str]]):
    if DRY_RUN_SETTING:
        return

    ids = {role["id"] for role in role_assigments_json}
    if not ids:
        log.info(f"Did not find any role assignments to delete in subscription {sub_id}")
        return

    # --include-inherited flag will also try to delete the role assignment from the management group scope if possible
    az(f"role assignment delete --ids {space_separated(ids)} --subscription {sub_id} --include-inherited --yes")


def find_diagnostic_settings(sub_id: str, sub_name: str, control_plane_ids: set) -> dict[str, set[str]]:
    """Returns mapping of resource ID to list of LFO diagnostic settings"""

    resource_ids = list_resources(sub_id, sub_name)
    resource_count = len(resource_ids)
    resource_ds_map = defaultdict(set)
    if not resource_ids:
        return resource_ds_map

    ds_count = 0
    diagnostic_settings_filter = " || ".join(f"name == '{DIAGNOSTIC_SETTING_PREFIX}{id}'" for id in control_plane_ids)
    with ThreadPoolExecutor(DIAGNOSTIC_SETTING_THREAD_POOL_SIZE) as tpe:
        ds_futures = [
            tpe.submit(
                az,
                f'monitor diagnostic-settings list --resource "{resource_id}" --query "[?{diagnostic_settings_filter}].name"',
            )
            for resource_id in resource_ids
        ]
        progress_spinner(resource_count, lambda: sum(f.done() for f in ds_futures))

    for resource_id, ds_future in zip(resource_ids, ds_futures):
        try:
            ds_names = json.loads(ds_future.result(timeout=MAX_WAIT_TIME))
        except concurrent.futures.TimeoutError:
            log.error(f"Timeout searching for diagnostic settings in resource {resource_id}")
            continue

        for ds_name in ds_names:
            resource_ds_map[resource_id].add(ds_name)
            ds_count += 1

    log.info(f"Found {ds_count} diagnostic settings to remove (searched through {resource_count} resources)")
    return resource_ds_map


def delete_diagnostic_setting(sub_id: str, resource_id: str, ds_name: str):
    if DRY_RUN_SETTING:
        return

    az(f"monitor diagnostic-settings delete --name {ds_name} --resource {resource_id} --subscription {sub_id}")


def init_resource_group_delete(sub_id_to_name: dict[str, str], sub_to_rg_deletions: SubIdToResourceGroupsDict):
    for sub_id, rg_set in sub_to_rg_deletions.items():
        sub_name = sub_id_to_name[sub_id]
        log.info(f"Deleting artifacts in {sub_name} ({sub_id})")
        start_resource_group_delete(sub_id, rg_set)


def do_role_assignment_delete(sub_role_assignment_deletions: SubIdToRoleAssignmentsDict):
    if not sub_role_assignment_deletions:
        return

    delete_log = "Deleting role assignments..."
    log.info(dry_run_of(delete_log) if DRY_RUN_SETTING else delete_log)

    with ThreadPoolExecutor(THREAD_POOL_SIZE) as tpe:
        futures: list[Future[None]] = []
        for sub_id, role_assignments_json in sub_role_assignment_deletions.items():
            if role_assignments_json:
                futures.append(tpe.submit(delete_role_assignments, sub_id, role_assignments_json))
        progress_spinner(len(futures), lambda: sum(f.done() for f in futures))


def do_diagnostic_setting_delete(sub_diagnostic_setting_deletions: SubIdToResourceIdToDiagSettingsDict):
    if not sub_diagnostic_setting_deletions:
        return

    delete_log = "Deleting diagnostic settings..."
    log.info(dry_run_of(delete_log) if DRY_RUN_SETTING else delete_log)

    with ThreadPoolExecutor(DIAGNOSTIC_SETTING_THREAD_POOL_SIZE) as tpe:
        futures: list[Future[None]] = []
        for sub_id, resource_ds_map in sub_diagnostic_setting_deletions.items():
            for resource_id, ds_names in resource_ds_map.items():
                for ds_name in ds_names:
                    futures.append(tpe.submit(delete_diagnostic_setting, sub_id, resource_id, ds_name))

        progress_spinner(len(futures), lambda: sum(f.done() for f in futures))


def start_resource_group_delete(sub_id: str, resource_group_set: set[str]):
    for rg in resource_group_set:
        deletion_log = f"Starting resource group '{rg}' deletion in background since it can take some time... "
        if DRY_RUN_SETTING:
            log.info(dry_run_of(deletion_log))
            return

        log.info(deletion_log)

        delete_resource_group(sub_id, rg)


def wait_for_resource_group_deletion(sub_to_rg_deletions: SubIdToResourceGroupsDict):
    log_msg = "Checking resource group deletion status (this can take 15+ minutes depending on number of resources in the groups)... "
    log.info(f"{dry_run_of(log_msg) if DRY_RUN_SETTING else log_msg}")

    if DRY_RUN_SETTING:
        return

    total_resource_count = 0
    for sub_id, rg_set in sub_to_rg_deletions.items():
        for rg in rg_set:
            total_resource_count += num_resources_in_group(sub_id, rg)

    num_resources = [total_resource_count]
    last_check = [datetime.now()]

    def current_resources_left():
        if (datetime.now() - last_check[0]).total_seconds() > RESOURCE_GROUP_DELETION_POLLING_DELAY:
            last_check[0] = datetime.now()
            with ThreadPoolExecutor(THREAD_POOL_SIZE) as tpe:
                futures = [
                    tpe.submit(num_resources_in_group, sub_id, rg)
                    for sub_id, rg_set in sub_to_rg_deletions.items()
                    for rg in rg_set
                ]
            num_resources[0] = sum(f.result() for f in futures)
        return total_resource_count - num_resources[0]

    progress_spinner(total_resource_count, current_resources_left)


# ===== User Interaction =====  #
def confirm_uninstall(
    sub_id_to_name: dict[str, str],
    sub_to_rg_deletions: SubIdToResourceGroupsDict,
    sub_to_role_assignment_deletions: SubIdToRoleAssignmentsDict,
    sub_to_resource_id_to_diag_setting_deletions: SubIdToResourceIdToDiagSettingsDict,
):
    """
    Displays summary of what will be deleted and prompts user for confirmation.
    Returns if user confirms, exits program otherwise
    """
    summary = uninstall_summary(
        sub_id_to_name,
        sub_to_rg_deletions,
        sub_to_role_assignment_deletions,
        sub_to_resource_id_to_diag_setting_deletions,
    )
    log.warning(summary)

    if SKIP_PROMPTS_SETTING:
        return

    choice = input("Continue? (y/n): ").lower().strip()
    while choice not in ["y", "n"]:
        choice = input("Continue? (y/n): ").lower().strip()

    if choice == "n":
        log.info("Exiting.")
        raise SystemExit(0)


def choose_rgs_to_delete(resource_groups_in_sub: set[str]) -> set[str]:
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
            return set()

        chosen_rg = (
            input("Please enter a valid resource group name from the list above, '*', or '-' \n: ").strip().lower()
        )

    return {chosen_rg}


def identify_rgs_to_delete(sub_id: str, sub_name: str, resource_groups_in_sub: set[str]) -> set[str]:
    """
    For given subscription, prompt the user to choose which resource group (or all) to delete if there's multiple.
    Returns a set of resource groups to delete
    """

    log.info(
        f"Found resource group(s) with log forwarding artifacts in '{sub_name}' ({sub_id}): {indented_log_of(resource_groups_in_sub)}"
    )

    return choose_rgs_to_delete(resource_groups_in_sub)


def mark_rg_deletions(
    sub_id_to_name: dict[str, str], sub_to_rgs_found: SubIdToResourceGroupsDict, is_control_plane_found: bool
) -> SubIdToResourceGroupsDict:
    """
    Based on resources found and user input, build a deletion plan for resource groups.
    Prompts user for input if multiple LFO resource groups are found in a sub.
    If control plane was found, this will also incorporate forwarder resource groups in all accessible subscriptions.
    Returns mapping of subscription ID to resource groups within it to delete.
    """

    sub_id_to_rg_deletions: SubIdToResourceGroupsDict = defaultdict(set)
    if not sub_to_rgs_found:
        log.info("Did not find any Datadog log forwarding artifacts")
        return sub_id_to_rg_deletions

    for sub_id, rg_set in sub_to_rgs_found.items():
        sub_name = sub_id_to_name[sub_id]
        rgs_to_delete = identify_rgs_to_delete(sub_id, sub_name, rg_set)
        if any(rgs_to_delete):
            sub_id_to_rg_deletions[sub_id].update(rgs_to_delete)

        if is_control_plane_found:
            # Search for forwarder resource groups with same name as control plane resource groups
            sub_to_log_forwarder_rg_deletions = mark_log_forwarder_env_rg_deletions(
                sub_to_rgs_found, rgs_to_delete, sub_id_to_rg_deletions
            )
            # Merge them into the deletion plan if found
            for sub_id, forwarder_rgs in sub_to_log_forwarder_rg_deletions.items():
                sub_id_to_rg_deletions[sub_id].update(forwarder_rgs)

        if rgs_to_delete and not is_control_plane_found:
            # Don't prompt the user for every subscription if we're showing resource forwader env resource groups - it would be many prompts.
            # We will delete the chosen resource group from all subs it was found in.
            break

    return sub_id_to_rg_deletions


def mark_log_forwarder_env_rg_deletions(
    sub_id_to_rgs_found: SubIdToResourceGroupsDict,
    rgs_to_delete: set[str],
    sub_id_to_rg_deletions: SubIdToResourceGroupsDict,
) -> SubIdToResourceGroupsDict:
    """
    Returns mapping of subscription ID to log forwarder resource groups to delete.
    Checks accessible subscriptions to see if they contain the resource group(s) that the user chose to delete.
    """
    sub_to_log_forwarder_rg_deletions: SubIdToResourceGroupsDict = defaultdict(set)

    # Check if other subs have the resource groups that the user chose to delete, mark them for deletion if so.
    # If the control plane resource group still exists, this loop will also mark forwarder env resource groups for deletion.
    for sub_id in sub_id_to_rgs_found:
        for rg in rgs_to_delete:
            if sub_has_rg(sub_id, rg) and rg not in sub_id_to_rg_deletions.get(sub_id, set()):
                sub_to_log_forwarder_rg_deletions[sub_id].add(rg)

    return sub_to_log_forwarder_rg_deletions


def mark_lfo_id_deletions(
    sub_to_rg_deletions: SubIdToResourceGroupsDict, rg_to_lfo_ids: ResourceGroupToLfoIdsDict
) -> set[str]:
    """Based on the control plane resource groups the user selected to delete, return the control plane IDs to target for deletion"""

    if CONTROL_PLANE_ID_SETTING:
        # User specified the control plane ID to search for
        return {CONTROL_PLANE_ID_SETTING}

    return {
        lfo_id for rg_deletions in sub_to_rg_deletions.values() for rg in rg_deletions for lfo_id in rg_to_lfo_ids[rg]
    }


def mark_role_assignment_deletions(
    sub_id_to_name: dict[str, str], lfo_id_deletions: set[str]
) -> SubIdToRoleAssignmentsDict:
    role_assignment_deletions = defaultdict(list)

    if lfo_id_deletions:
        log.info("Searching for Datadog role assignments in subscriptions... ")
        role_assignment_deletions = find_role_assignments(sub_id_to_name, lfo_id_deletions)
    else:
        log.info("Skipping role assignment search since no log forwarding IDs were found")

    log.info(
        "Searching for role assignments that could be leftover from previous installations (labeled 'Unknown' in Azure)... "
    )
    unknown_role_assignments = find_unknown_role_assignments(sub_id_to_name)
    for sub_id, unknown_role_assignment_json in unknown_role_assignments.items():
        if unknown_role_assignment_json:
            role_assignment_deletions[sub_id].extend(unknown_role_assignment_json)

    return role_assignment_deletions


def mark_diagnostic_setting_deletions(
    sub_id_to_name: dict[str, str], lfo_id_deletions: set[str]
) -> SubIdToResourceIdToDiagSettingsDict:
    sub_diagnostic_setting_deletions = defaultdict(dict)
    log.info("Searching for Datadog diagnostic settings in subscriptions... ")
    for sub_id, sub_name in sub_id_to_name.items():
        resource_ds_map = find_diagnostic_settings(sub_id, sub_name, lfo_id_deletions)
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
        help="Specify subscription ID to search for log forwarding artifacts. This can save time if you know the subscription where log forwarding was installed.",
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Skip all user prompts. This will delete all detected installations without confirmation",
    )
    parser.add_argument(
        "-cp",
        "--control-plane",
        type=str,
        help="Specify control plane ID to search for. If specified, only this control plane ID will be searched for artifacts",
    )
    args = parser.parse_args()

    global DRY_RUN_SETTING, SKIP_PROMPTS_SETTING, CONTROL_PLANE_ID_SETTING, SUBSCRIPTION_ID_SETTING
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

    if args.control_plane:
        CONTROL_PLANE_ID_SETTING = args.control_plane
        log.info(f"Will search for log forwarding artifacts related to ID '{CONTROL_PLANE_ID_SETTING}'")

    if args.subscription:
        SUBSCRIPTION_ID_SETTING = args.subscription
        log.info(f"Will begin search within subscription '{SUBSCRIPTION_ID_SETTING}'")

    return args


def main():
    """
    Overview:
    1) Fetch subscriptions accessible by current user
        - If user specified the subscription ID param, filter down subscription searches in 2a/2b to only that subscription.
    2a) For each subscription, search for LFO control planes. If found:
        - Map the subscription to control plane resource group
        - Map the control plane resource group to LFO ID
    2b) If a control plane was not found, search for log forwarder resource groups in each subscription. If found:
        - Map the subscription to log forwarder resource group
        - Map the log forwarder resource group to LFO ID
    3) For each subscription, determine which LFO resource groups need to be deleted
        - User will be prompted to confirm which resource group(s) to delete.
        - If the user specified a control plane ID, only resource groups associated to that ID will be displayed.
    4) Based on the resource groups selected for deletion, note the corresponding control plane IDs
        - If a control plane was found, search for forwarder resource groups in all accessible subscriptions. Mark them for deletion if found.
    5) Based on control plane IDs, find corresponding role assignments and diagnostic settings
    6) Display summary of what will be deleted to user. May prompt for confirmation.
    7) Delete role assignments, diagnostic settings, and resource groups.
    8) Confirm the artifacts are deleted successfully.
    """

    parse_args()

    sub_id_to_name = list_users_subscriptions()

    sub_to_resource_groups_found: SubIdToResourceGroupsDict = defaultdict(set)
    rg_to_lfo_ids: SubIdToResourceGroupsDict = defaultdict(set)
    control_plane_found = False
    log_forwarders_found = False

    sub_id_to_control_plane_rgs, control_plane_rg_to_lfo_id = find_all_control_planes(sub_id_to_name)

    control_plane_found = len(control_plane_rg_to_lfo_id) > 0

    if control_plane_found:
        log.info("Found Datadog log forwarding control plane(s)")
        # Build resource group deletion plan based on control plane resource group name.
        # We will search other subscriptions for forwarders using control plane resource group name.
        sub_to_resource_groups_found = sub_id_to_control_plane_rgs
        rg_to_lfo_ids = control_plane_rg_to_lfo_id
    else:
        # Search for forwarder app environments to delete
        log.info("Did not find any control planes")
        sub_id_to_forwarder_rgs, forwarder_rgs_to_lfo_id = find_all_forwarder_envs(sub_id_to_name)
        log_forwarders_found = len(forwarder_rgs_to_lfo_id) > 0

        if log_forwarders_found:
            sub_to_resource_groups_found = sub_id_to_forwarder_rgs
            rg_to_lfo_ids = forwarder_rgs_to_lfo_id

    if not control_plane_found and not log_forwarders_found and not CONTROL_PLANE_ID_SETTING:
        log.info("Could not find any Datadog control plane resource groups or log forwarder resource groups to delete.")
        log.info("'Unknown' role assignments may be leftover from prior installs, searching for them...")
        unknown_role_assignments = find_unknown_role_assignments(sub_id_to_name)
        if unknown_role_assignments:
            log.info("Found 'Unknown' role assignments, deleting them...")
            do_role_assignment_delete(unknown_role_assignments)
        else:
            log.info("No 'Unknown' role assignments found, exiting.")
        raise SystemExit(0)

    sub_to_rg_deletions = mark_rg_deletions(sub_id_to_name, sub_to_resource_groups_found, control_plane_found)

    if not any(sub_to_rg_deletions.values()) and not CONTROL_PLANE_ID_SETTING:
        log.info("Could not find any resource groups to delete.")
        raise SystemExit(0)

    lfo_id_deletions = mark_lfo_id_deletions(sub_to_rg_deletions, rg_to_lfo_ids)
    sub_to_role_assignment_deletions = mark_role_assignment_deletions(sub_id_to_name, lfo_id_deletions)
    sub_to_resource_id_to_diag_setting_deletions = mark_diagnostic_setting_deletions(sub_id_to_name, lfo_id_deletions)

    confirm_uninstall(
        sub_id_to_name,
        sub_to_rg_deletions,
        sub_to_role_assignment_deletions,
        sub_to_resource_id_to_diag_setting_deletions,
    )

    init_resource_group_delete(sub_id_to_name, sub_to_rg_deletions)
    do_role_assignment_delete(sub_to_role_assignment_deletions)
    do_diagnostic_setting_delete(sub_to_resource_id_to_diag_setting_deletions)

    if sub_to_rg_deletions:
        wait_for_resource_group_deletion(sub_to_rg_deletions)

    # Execute a second delete because Azure sometimes deletes all the resources within, but not the group itself
    for sub_id, rg_set in sub_to_rg_deletions.items():
        for resource_group in rg_set:
            delete_resource_group(sub_id, resource_group)

    log.info("Uninstall done! Exiting.")


if __name__ == "__main__":
    basicConfig(level=INFO)
    main()
