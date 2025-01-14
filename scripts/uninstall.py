#!/usr/bin/env python
# Remove DataDog Log Forwarding Orchestration from an Azure environment

import argparse
import json
import subprocess
from collections import defaultdict
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging import INFO, WARNING, basicConfig, getLogger
from time import sleep
from typing import Any, Final

getLogger("azure").setLevel(WARNING)
log = getLogger("uninstaller")

DRY_RUN = False

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
    "Microsoft.Cdn": {"cdnwebapplicationfirewallpolicies", "profiles", "profiles/endpoints"},
    "Microsoft.Chaos": {"experiments"},
    "Microsoft.ClassicNetwork": {"networksecuritygroups"},
    "Microsoft.CodeSigning": {"codesigningaccounts"},
    "Microsoft.CognitiveServices": {"accounts"},
    "Microsoft.Communication": {"CommunicationServices"},
    "microsoft.community": {"communityTrainings"},
    "Microsoft.Compute": {"virtualMachines"},
    "Microsoft.ConfidentialLedger": {"ManagedCCF", "ManagedCCFs"},
    "Microsoft.ConnectedCache": {"CacheNodes", "enterpriseMccCustomers", "ispCustomers"},
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
    "Microsoft.EventGrid": {"domains", "namespaces", "partnerNamespaces", "partnerTopics", "systemTopics", "topics"},
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
    "Microsoft.MachineLearningServices": {"", "registries", "workspaces", "workspaces/onlineEndpoints"},
    "Microsoft.ManagedNetworkFabric": {"networkDevices"},
    "Microsoft.Media": {
        "mediaservices",
        "mediaservices/liveEvents",
        "mediaservices/streamingEndpoints",
        "videoanalyzers",
    },
    "Microsoft.Monitor": {"accounts"},
    "Microsoft.NetApp": {"netAppAccounts/capacityPools", "netAppAccounts/capacityPools/volumes"},
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
    "microsoft.network": {"bastionHosts", "p2svpngateways", "virtualnetworkgateways", "vpngateways"},
    "Microsoft.NetworkAnalytics": {"DataProducts"},
    "Microsoft.NetworkCloud": {"bareMetalMachines", "clusterManagers", "clusters", "storageAppliances"},
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
    "Microsoft.SignalRService": {"SignalR", "SignalR/replicas", "WebPubSub", "WebPubSub/replicas"},
    "microsoft.singularity": {"accounts"},
    "Microsoft.Sql": {"managedInstances", "managedInstances/databases", "servers/databases"},
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


# ===== Utility ===== #
def first_key_of(d: dict[str, Any]) -> str:
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

    progress_bar_length = 40
    progress = current / total
    done_bar = int(progress_bar_length * progress)
    leftover_bar = progress_bar_length - done_bar
    percent_done = f"{progress * 100:.0f}%"
    print(f"[{'#' * done_bar + '-' * (leftover_bar)}] {current}/{total} ({percent_done})", end="\r", flush=True)

    if current == total:
        print("\nDone!")


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
    deletion_summary = f"{SEPARATOR}{dry_run_of(header) if DRY_RUN else header}:\n"

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

    max_retries = 6
    delay = 2  # seconds
    az_cmd = f"az {cmd}"

    for attempt in range(max_retries):
        try:
            result = subprocess.run(az_cmd, shell=True, check=True, text=True, capture_output=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            stderr = str(e.stderr)
            if (
                "resource list" in cmd
                and "--resource-group" in cmd
                and '--query "length([])"' in cmd
                and "ResourceGroupNotFound" in stderr
            ):
                return "0"
            if "TooManyRequests" in stderr:
                if attempt < max_retries - 1:
                    sleep(delay)
                    delay *= 2
                    log.warning(f"Azure throttling ongoing. Retrying in {delay} seconds...")
                    continue

                log.error("Rate limit exceeded. Please wait a few minutes and try again.")
                raise SystemExit(1) from e

            log.error(f"Error running Azure CLI command:\n{e.stderr}")
            raise SystemExit(1) from e

    raise SystemExit(1)  # unreachable


def list_users_subscriptions() -> dict:
    log.info("Searching for log forwarding installations in all subscriptions accessible by current user... ")

    all_subs_json = json.loads(az("account list --output json"))
    print(f"Found {len(all_subs_json)}")

    return {sub["id"]: sub["name"] for sub in all_subs_json}


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
    """Queries for LFO control planes in single subscription, returns mapping of resource group name to control plane storage account name"""

    log.info(f"Searching for Datadog log forwarding instance in subscription '{sub_name}' ({sub_id})... ")
    cmd = f"storage account list --subscription {sub_id} --query \"[?starts_with(name,'{CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX}')].{{resourceGroup:resourceGroup, name:name}}\" --output json"
    storage_accounts_json = json.loads(az(cmd))

    lfo_install_map = {account["resourceGroup"]: account["name"] for account in storage_accounts_json}
    print(f"Found {len(lfo_install_map)}")

    return lfo_install_map


def find_all_control_planes(sub_id_to_name: dict) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """
    Queries for all LFO control planes that the user has access to.
    Returns 2 dictionaries - a subcription ID to resource group mapping and a resource group to control plane ID mapping
    """

    sub_to_rg = defaultdict(list)
    rg_to_lfo_id = defaultdict(list)

    for sub_id, sub_name in sub_id_to_name.items():
        control_planes = find_sub_control_planes(sub_id, sub_name)
        if not control_planes:
            continue

        for resource_group_name, storage_account_name in control_planes.items():
            sub_to_rg[sub_id].append(resource_group_name)
            rg_to_lfo_id[resource_group_name].append(
                storage_account_name.removeprefix(CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX)
            )

    return sub_to_rg, rg_to_lfo_id


def find_role_assignments(sub_id: str, sub_name: str, control_plane_ids: set) -> list[dict[str, str]]:
    """Returns JSON array of role assignments (properties = id, roleDefinitionName, principalId)"""

    log.info(f"Looking for DataDog role assignments in {sub_name} ({sub_id})... ")

    description_filter = " || ".join(f"description == 'ddlfo{id}'" for id in control_plane_ids)
    role_assignment_json = json.loads(
        az(
            f'role assignment list --all --subscription {sub_id} --query "[?description != null && ({description_filter})].{{id: id, roleDefinitionName: roleDefinitionName, principalId: principalId}}" --output json'
        )
    )

    print(f"Found {len(role_assignment_json)} role assignment(s)")

    return role_assignment_json


def delete_role_assignments(sub_id: str, role_assigments_json: list[dict[str, str]]):
    if DRY_RUN:
        return

    ids = {role["id"] for role in role_assigments_json}
    if not ids:
        log.info(f"Did not find any role assignments to delete in subscription {sub_id}")
        return

    # --include-inherited flag will also try to delete the role assignment from the management group scope if possible
    az(f"role assignment delete --ids {space_separated(ids)} --subscription {sub_id} --include-inherited --yes")


def find_diagnostic_settings(sub_id: str, sub_name: str, control_plane_ids: set) -> dict[str, list[str]]:
    """Returns mapping of resource ID to list of LFO diagnostic settings"""

    resource_ids = list_resources(sub_id, sub_name)
    resource_count = len(resource_ids)
    resource_ds_map = defaultdict(list)
    if not resource_ids:
        return resource_ds_map

    log.info(f"Looking for DataDog log forwarding diagnostic settings in {sub_name} ({sub_id})... ")
    ds_count = 0
    diagnostic_settings_filter = " || ".join(f"name == '{DIAGNOSTIC_SETTING_PREFIX}{id}'" for id in control_plane_ids)
    for i, resource_id in enumerate(resource_ids, start=1):
        print_progress(i, resource_count)
        ds_names = json.loads(
            az(
                f'monitor diagnostic-settings list --resource {resource_id} --query "[?{diagnostic_settings_filter}].name"'
            )
        )
        for ds_name in ds_names:
            resource_ds_map[resource_id].append(ds_name)
            ds_count += 1

    log.info(f"Found {ds_count} diagnostic settings to remove (searched through {resource_count} resources)")
    return resource_ds_map


def delete_diagnostic_setting(sub_id: str, resource_id: str, ds_name: str):
    if DRY_RUN:
        return

    az(f"monitor diagnostic-settings delete --name {ds_name} --resource {resource_id} --subscription {sub_id}")


def delete_roles_diag_settings(
    sub_id: str,
    sub_role_assignment_deletions: dict[str, list[dict[str, str]]],
    sub_diagnostic_setting_deletions: dict[str, dict[str, list[str]]],
):
    delete_log = "Deleting role assignments and diagnostic settings"
    log.info(f"{dry_run_of(delete_log) if DRY_RUN else delete_log}")

    with ThreadPoolExecutor(100) as tpe:
        futures = []
        for sub_id, role_assignments_json in sub_role_assignment_deletions.items():
            if role_assignments_json:
                futures.append(tpe.submit(delete_role_assignments, sub_id, role_assignments_json))
        for sub_id, resource_ds_map in sub_diagnostic_setting_deletions.items():
            for resource_id, ds_names in resource_ds_map.items():
                for ds_name in ds_names:
                    futures.append(tpe.submit(delete_diagnostic_setting, sub_id, resource_id, ds_name))

    num_completed = 0
    print_progress(num_completed, len(futures))
    for _ in as_completed(futures):
        num_completed += 1
        print_progress(num_completed, len(futures))


def start_resource_group_delete(sub_id: str, resource_group_list: list[str]):
    for resource_group in resource_group_list:
        rg_deletion_log = (
            f"Starting resource group {resource_group} deletion in background since it can take some time... "
        )
        if DRY_RUN:
            log.info(dry_run_of(rg_deletion_log))
            return

        log.info(rg_deletion_log)

        # --no-wait will initiate delete and immediately return
        az(f"group delete --subscription {sub_id} --name {resource_group} --yes --no-wait")


def check_resource_group_delete_status(sub_to_rg_deletions: dict[str, list[str]]):
    log_msg = "Checking resource group deletion status... "
    log.info(f"{dry_run_of(log_msg) if DRY_RUN else log_msg}")

    if DRY_RUN:
        return

    total_resource_count = 0
    for sub_id, rg_list in sub_to_rg_deletions.items():
        for rg in rg_list:
            total_resource_count += num_resources_in_group(sub_id, rg)

    while True:
        leftover_resource_count = 0
        for sub_id, rg_list in sub_to_rg_deletions.items():
            for rg in rg_list:
                leftover_resource_count += num_resources_in_group(sub_id, rg)
        deleted_resource_count = total_resource_count - leftover_resource_count
        print_progress(deleted_resource_count, total_resource_count)
        if leftover_resource_count == 0:
            break
        sleep(5)


# ===== User Interaction =====  #
def confirm_uninstall(
    sub_to_rg_deletions: dict[str, list[str]],
    sub_id_to_name: dict[str, str],
    role_assignment_deletions: dict[str, list[dict[str, str]]],
    sub_diagnostic_setting_deletions: dict[str, dict[str, list[str]]],
) -> bool:
    """Displays summary of what will be deleted and prompts user for confirmation. Returns true if user confirms, false otherwise"""
    summary = uninstall_summary(
        sub_to_rg_deletions, sub_id_to_name, role_assignment_deletions, sub_diagnostic_setting_deletions
    )
    log.warning(summary)

    choice = input("Continue? (y/n): ").lower().strip()
    while choice not in ["y", "n"]:
        choice = input("Continue? (y/n): ").lower().strip()

    return choice == "y"


def choose_resource_groups_to_delete(resource_groups_in_sub: list[str]) -> list[str]:
    """Given list of resource groups, prompt the user to select what to delete. Returns what was selected"""

    prompt = """
    Enter the resource group name you would like to remove
    - To remove all of them, enter *
    - To remove nothing, enter -
    : """
    chosen_rg = input(prompt).strip().lower()

    while chosen_rg not in resource_groups_in_sub:
        if chosen_rg == "*":
            return resource_groups_in_sub
        if chosen_rg == "-":
            return []

        chosen_rg = input("Please enter a valid resource group name from the list above, *, or - \n: ").strip().lower()

    return [chosen_rg]


def identify_resource_groups_to_delete(sub_id: str, sub_name: str, resource_groups_in_sub: list[str]) -> list[str]:
    """For given subscription, prompt the user to choose which resource group (or all) to delete if there's multiple. Returns a list of resource groups to delete"""

    if len(resource_groups_in_sub) == 1:
        log.info(f"Found single resource group with log forwarding artifact: '{resource_groups_in_sub[0]}'")
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
        log.info("Did not find any DataDog log forwarding installations")
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
            sub_id_to_rg_deletions[sub_id] = rgs_to_delete

    return sub_id_to_rg_deletions


def mark_control_plane_deletions(
    sub_to_rg_deletions: dict[str, list[str]], rg_to_lfo_id: dict[str, list[str]]
) -> set[str]:
    """Based on the resource groups the user selected previously, return the control plane IDs to target for deletion"""

    control_plane_ids_to_delete = set()

    for sub in sub_to_rg_deletions:
        for rg in sub_to_rg_deletions[sub]:
            lfo_ids = rg_to_lfo_id[rg]
            for id in lfo_ids:
                control_plane_ids_to_delete.add(id)

    return control_plane_ids_to_delete


def mark_role_assignment_deletions(
    sub_to_rg_deletions: dict[str, list[str]], sub_id_to_name: dict, control_plane_id_deletions: set[str]
) -> dict[str, list[dict[str, str]]]:
    role_assignment_deletions = defaultdict(list)
    for sub_id in sub_to_rg_deletions:
        sub_name = sub_id_to_name[sub_id]
        role_assignment_json = find_role_assignments(sub_id, sub_name, control_plane_id_deletions)
        if role_assignment_json:
            role_assignment_deletions[sub_id] = role_assignment_json

    return role_assignment_deletions


def mark_diagnostic_setting_deletions(
    sub_to_rg_deletions: dict[str, list[str]], sub_id_to_name: dict, control_plane_id_deletions: set[str]
) -> dict[str, dict[str, list[str]]]:
    sub_diagnostic_setting_deletions = defaultdict(dict)
    for sub_id in sub_to_rg_deletions:
        sub_name = sub_id_to_name[sub_id]
        resource_ds_map = find_diagnostic_settings(sub_id, sub_name, control_plane_id_deletions)
        if resource_ds_map:
            sub_diagnostic_setting_deletions[sub_id] = resource_ds_map

    return sub_diagnostic_setting_deletions


def main():
    """
    Overview:
    1) Fetch all subscriptions accessible by the current user.
    2) For each subscription, search for LFO control planes. If found:
        - Map the subscription to control plane resource group
        - Map the resource group to storage account mapping
    3) For each subscription, determine which LFO resource groups need to be deleted
        - If there is only one resource group in the sub, mark it for deletion
        - If there are multiple, user input will be required to disambiguate
    4) Based on the resource groups marked for deletion, note the corresponding control plane IDs
    5) Based on control plane ID, find corresponding role assignments and diagnostic settings
    6) Display summary of what will be deleted to user. Prompt for confirmation.
    7) Delete role assignments, diagnostic settings, and resource groups.
    """

    global DRY_RUN
    parser = argparse.ArgumentParser(
        description="Uninstall DataDog Log Forwarding Orchestration from an Azure environment"
    )
    parser.add_argument(
        "-d",
        "--dry-run",
        action="store_true",
        help="Run the script in dry-run mode. No changes will be made to the Azure environment",
    )
    args = parser.parse_args()

    if args.dry_run:
        DRY_RUN = True
        log.info("Dry run enabled, no changes will be made")
    else:
        log.warning(
            "Deletion of resource groups and ALL resources within them will occur as a result of the uninstall process."
        )
        log.warning(
            "If you have created any Azure resources in resource groups managed by DataDog log forwarding, they will be deleted. Perform backups if necessary!"
        )

    sub_id_to_name = list_users_subscriptions()
    sub_id_to_rgs, rg_to_lfo_id = find_all_control_planes(sub_id_to_name)
    sub_to_rg_deletions = mark_rg_deletions_per_sub(sub_id_to_name, sub_id_to_rgs)

    if not sub_to_rg_deletions or not any(sub_to_rg_deletions.values()):
        log.info("Could not find any resource groups to delete as part of uninstall process. Exiting.")
        raise SystemExit(0)

    control_plane_id_deletions = mark_control_plane_deletions(sub_to_rg_deletions, rg_to_lfo_id)

    sub_role_assignment_deletions = mark_role_assignment_deletions(
        sub_to_rg_deletions, sub_id_to_name, control_plane_id_deletions
    )
    sub_diagnostic_setting_deletions = mark_diagnostic_setting_deletions(
        sub_to_rg_deletions, sub_id_to_name, control_plane_id_deletions
    )

    confirmed = confirm_uninstall(
        sub_to_rg_deletions, sub_id_to_name, sub_role_assignment_deletions, sub_diagnostic_setting_deletions
    )
    if not confirmed:
        log.info("Exiting.")
        raise SystemExit(0)

    for sub_id, rg_list in sub_to_rg_deletions.items():
        sub_name = sub_id_to_name[sub_id]
        log.info(f"{SEPARATOR}Deleting artifacts in {sub_name} ({sub_id})")
        start_resource_group_delete(sub_id, rg_list)
        delete_roles_diag_settings(sub_id, sub_role_assignment_deletions, sub_diagnostic_setting_deletions)

    check_resource_group_delete_status(sub_to_rg_deletions)

    log.info("Uninstall done! Exiting.")


if __name__ == "__main__":
    basicConfig(level=INFO)
    main()
