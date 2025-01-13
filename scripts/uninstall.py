#!/usr/bin/env python
# Remove DataDog Log Forwarding Orchestration from an Azure environment
# Last updated: Jan 2025

from asyncio import run
from collections import defaultdict
from logging import INFO, WARNING, basicConfig, getLogger
from typing import Any, Iterable, Final
import argparse
import json 
import subprocess

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
def first_key_of(d: dict[str,Any]) -> str:
    return next(iter(d))

def newline_spaced(dict: dict) -> str:
    items = dict.items()
    formatted = "\n".join(f"\t{key} | {value}" for key, value in items)
    return f"\n{formatted}\n"

def space_separated(iter: Iterable) -> str:
    return " ".join(iter)

def comma_separated_quoted(iter: Iterable) -> str:
    return ", ".join(f"\"{i}\"" for i in iter)

def indented_log_of(iter: Iterable) -> str:
    formatted = "\n".join(f"\t- {item}" for item in iter)
    return f"\n{formatted}\n"

def formatted_number(n: int) -> str:
    return f"{n:,}"

def dry_run_of(s: str) -> str:
    msg = s[0].lower() + s[1:]
    return f"DRY RUN | Would be {msg}"

def print_progress(current: int, total: int):
    progress_bar_length = 40
    progress = current / total
    done_bar = int(progress_bar_length * progress)
    leftover_bar = progress_bar_length - done_bar
    percent_done = f"{progress * 100:.0f}%"
    print(f"[{'#' * done_bar + '-' * (leftover_bar)}] {current}/{total} ({percent_done})", end='\r', flush=True)

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
    ds_set = set() 
    for _, ds_list in resource_ds_map.items():
        ds_set.update(ds_list)

    summary = f"\tDiagnostic Settings: {'None' if not ds_set else ''}\n"
    for ds in ds_set:
        summary += f"\t\t- {ds}\n"
    
    return summary

def resource_group_summary(resource_groups: list[str]) -> str:
    summary = f"\tResource Groups: {'None' if not resource_groups else ''}\n"
    for rg in resource_groups:
        summary += f"\t\t- {rg}\n"

    return summary

def uninstall_summary(sub_to_rg_deletions: dict[str,list[str]], 
                      sub_id_to_name: dict[str,str],
                      role_assignment_deletions: dict[str, list[Any]],
                      sub_diagnostic_setting_deletions: dict[str, dict[str,list[str]]]) -> str:
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

# ===== Command Execution ===== # 
def az(cmd: str) -> str:
    """Runs az command, returns stdout"""
    
    az_cmd = f"az {cmd}"
    
    try:
        result = subprocess.run(az_cmd, shell=True, check=True, text=True, capture_output=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running Azure CLI command:\n{e.stderr}")
        SystemExit(1)
        return ""

# ===== Azure Commands ===== #
def list_users_subscriptions() -> dict:
    log.info("Searching for log forwarding installations in all subscriptions accessible by current user... ")
    
    all_subs_json = json.loads(az("account list --output json"))
    print(f"Found {len(all_subs_json)}")

    return {sub["id"]: sub["name"] for sub in all_subs_json}

def list_resources(sub_id: str) -> set:
    log.info("Searching for resources in subscription... ")
    
    resource_ids = json.loads(az(f"resource list --subscription {sub_id} --query \"[?{ALLOWED_RESOURCE_TYPES_FILTER}].id\" --output json"))
    print(f"Found {formatted_number(len(resource_ids))} resource(s)")

    return set(resource_ids)

def find_sub_control_planes(sub_id: str, sub_name: str) -> dict[str,str]:
    """Queries for LFO control planes in single subscription, returns mapping of resource group name to control plane storage account name"""
    
    log.info(f"Searching for Datadog log forwarding instance in subscription '{sub_name}' ({sub_id})... ")
    cmd = f"storage account list --subscription {sub_id} --query \"[?starts_with(name,'{CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX}')].{{resourceGroup:resourceGroup, name:name}}\" --output json"
    storage_accounts_json = json.loads(az(cmd))

    lfo_install_map = {account["resourceGroup"] : account["name"] for account in storage_accounts_json} 
    print(f"Found {len(lfo_install_map)}")

    return lfo_install_map

def find_all_control_planes(sub_id_to_name: dict) -> tuple[dict[str,list[str]], dict[str,list[str]]]:
    """
    Queries for all LFO control planes that the user has access to.
    Returns 2 dictionaries - a subcription ID to resource group mapping and a resource group to storage account mapping
    """

    sub_to_rg = defaultdict(list)
    rg_to_storage = defaultdict(list)

    for sub_id, sub_name in sub_id_to_name.items():
        control_planes = find_sub_control_planes(sub_id, sub_name)
        if not control_planes:
            continue

        for resource_group_name, storage_account_name in control_planes.items():        
            sub_to_rg[sub_id].append(resource_group_name)
            rg_to_storage[resource_group_name].append(storage_account_name)
        
    return sub_to_rg, rg_to_storage

def find_role_assignments(sub_id: str, control_plane_ids: set) -> list[dict[str,str]]:
    """Returns JSON array of role assignments (properties = id, roleDefinitionName, principalId)"""
    
    log.info("Looking for DataDog role assignments... ")

    description_filter = " || ".join(f"description == 'ddlfo{id}'" for id in control_plane_ids)
    role_assignment_json = json.loads(az(f"role assignment list --all --subscription {sub_id} --query \"[?description != null && ({description_filter})].{{id: id, roleDefinitionName: roleDefinitionName, principalId: principalId}}\" --output json"))

    print(f"Found {len(role_assignment_json)} role assignment(s)")

    return role_assignment_json

def delete_role_assignments(sub_id: str, role_assigments_json: list[dict[str,str]]):
    ids = {role["id"] for role in role_assigments_json}
    if not ids:
        log.info(f"Did not find any role assignments to delete in subscription {sub_id}")
        return 
    
    role_deletion_log = "Deleting role assignments"
    if DRY_RUN:
        log.info(dry_run_of(role_deletion_log))
        return

    log.info(role_deletion_log)
    
    print_progress(0, 1)
    
    # --include-inherited flag will also try to delete the role assignment from the management group scope if possible
    az(f"role assignment delete --ids {space_separated(ids)} --subscription {sub_id} --include-inherited --yes")
    print_progress(1, 1)

def find_diagnostic_settings(sub_id: str, control_plane_ids: set) -> dict[str, list[str]]:
    """Returns mapping of resource ID to list of LFO diagnostic settings"""
    
    resource_ids = list_resources(sub_id)
    resource_count = len(resource_ids)
    resource_ds_map = defaultdict(list)
    if not resource_ids:
        return resource_ds_map
    
    log.info("Looking for DataDog log forwarding diagnostic settings... ")
    ds_count = 0
    diagnostic_settings_filter = " || ".join(f"name == '{DIAGNOSTIC_SETTING_PREFIX}{id}'" for id in control_plane_ids)
    for i, resource_id in enumerate(resource_ids, start = 1):        
        print_progress(i, resource_count)
        ds_names = json.loads(az(f"monitor diagnostic-settings list --resource {resource_id} --query \"[?{diagnostic_settings_filter}].name\""))
        for ds_name in ds_names:
            resource_ds_map[resource_id].append(ds_name)
            ds_count += 1
    
    log.info(f"Found {ds_count} diagnostic settings to remove across {resource_count} resources")
    return resource_ds_map
            
def delete_diagnostic_settings(sub_id: str, resource_setting_map: dict[str,list[str]]):    
    num_resources = len(resource_setting_map)
    ds_deletion_log = f"Deleting DataDog diagnostic settings from {num_resources} resources"
    if DRY_RUN:
        log.info(f"{dry_run_of(ds_deletion_log)}")
        return

    log.info(ds_deletion_log)
    for i, (resource_id, ds_names) in enumerate(resource_setting_map.items(), 1):
        print_progress(i, num_resources)
        for ds_name in ds_names:
            az(f"monitor diagnostic-settings delete --name {ds_name} --resource {resource_id} --subscription {sub_id}")

def delete_resource_group(sub_id: str, resource_group_list: list[str]):
    for resource_group in resource_group_list:
        rg_deletion_log = f"Deleting resource group {resource_group} (this could take a few minutes)... "
        if DRY_RUN:
            log.info(dry_run_of(rg_deletion_log))
            return
        
        log.info(rg_deletion_log)
        az(f"group delete --subscription {sub_id} --name {resource_group} --yes")
        print("Done!")

# ===== User Interaction =====  #
def confirm_uninstall(sub_to_rg_deletions: dict[str,list[str]], 
                      sub_id_to_name: dict[str,str],
                      role_assignment_deletions: dict[str, list[dict[str,str]]],
                      sub_diagnostic_setting_deletions: dict[str, dict[str,list[str]]]) -> bool:
    """Displays summary of what will be deleted and prompts user for confirmation. Returns true if user confirms, false otherwise"""
    summary = uninstall_summary(sub_to_rg_deletions, 
                                sub_id_to_name, 
                                role_assignment_deletions, 
                                sub_diagnostic_setting_deletions)
    log.warning(summary)
    
    choice = ""
    while choice not in ["y", "n"]:
        choice = input("Continue? (y/n): ").lower().strip()

    return choice == 'y'

def choose_resource_groups_to_delete(resource_groups_in_sub: list[str]) -> list[str]:
    """Given list of resource groups, prompt the user to select what to delete. Returns what was selected"""
    
    prompt = '''
    Enter the resource group name you would like to remove
    - To remove all of them, enter *
    - To remove nothing, enter -
    : '''
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
    
    log.info(f"Found multiple resource groups with log forwarding artifacts in '{sub_name}' ({sub_id}): {indented_log_of(resource_groups_in_sub)}")
    
    return choose_resource_groups_to_delete(resource_groups_in_sub)

def mark_rg_deletions_per_sub(sub_id_to_name: dict[str,str], 
                              sub_id_to_rgs: dict[str,list[str]]) -> dict[str, list[str]]:
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

def mark_control_plane_deletions(sub_to_rg_deletions: dict[str, list[str]], 
                                 rg_to_storage_account: dict[str,list[str]]) -> set[str]:
    '''Based on the resource groups the user selected previously, return the control plane IDs to target for deletion'''
    
    control_plane_ids_to_delete = set()
    
    for sub in sub_to_rg_deletions:
        for rg in sub_to_rg_deletions[sub]:
            storage_accounts = rg_to_storage_account[rg]
            for account in storage_accounts:
                control_plane_ids_to_delete.add(account[len(CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX):])
    
    return control_plane_ids_to_delete

async def main():
    '''
    Overview:
    1) Fetch all subscriptions accessible by the current user. Cache them
    2) For each subscription, search for LFO control planes. If found:
        - Cache the subscription to control plane resource group mapping 
        - Cache the resource group to storage account mapping
    3) For each subscription, determine which LFO resource groups need to be deleted
        - If there is only one resource group in the sub, mark it for deletion
        - If there are multiple, user input will be required to disambiguate
    4) Based on the resource groups marked for deletion, cache the corresponding control plane IDs
    5) Based on control plane ID, find corresponding role assignments and diagnostic settings
    6) Display summary of what will be deleted to user. Prompt for confirmation.
    7) Delete role assignments, diagnostic settings, and resource groups.
    '''

    global DRY_RUN
    parser = argparse.ArgumentParser(description="Uninstall DataDog Log Forwarding Orchestration from an Azure environment")
    parser.add_argument("-d", "--dry-run", action="store_true", help="Run the script in dry-run mode. No changes will be made to the Azure environment")
    args = parser.parse_args()

    if args.dry_run:
        DRY_RUN = True
        log.info("Dry run enabled, no changes will be made")
    else:
        log.warning("Deletion of resource groups and ALL resources within them will occur as a result of the uninstall process.")
        log.warning("If you have created any Azure resources in resource groups managed by DataDog log forwarding, they will be deleted. Perform backups if necessary!")

    sub_id_to_name = list_users_subscriptions()    
    sub_id_to_rgs, rg_to_storage_account = find_all_control_planes(sub_id_to_name)
    sub_to_rg_deletions = mark_rg_deletions_per_sub(sub_id_to_name, sub_id_to_rgs)

    if not sub_to_rg_deletions or not any(sub_to_rg_deletions.values()):
        log.info("Could not find any resource groups to delete as part of uninstall process. Exiting.")
        raise SystemExit(0)
    
    control_plane_id_deletions = mark_control_plane_deletions(sub_to_rg_deletions, rg_to_storage_account)    
    
    role_assignment_deletions = defaultdict(list) 
    sub_diagnostic_setting_deletions = defaultdict(dict) 
    for sub_id in sub_to_rg_deletions.keys():
        log.info(f"{SEPARATOR}Processing subscription '{sub_id_to_name[sub_id]}' ({sub_id})\n\n")
        role_assignment_json = find_role_assignments(sub_id, control_plane_id_deletions)
        if role_assignment_json:
            role_assignment_deletions[sub_id] = role_assignment_json
        
        resource_ds_map = find_diagnostic_settings(sub_id, control_plane_id_deletions)
        if resource_ds_map:
            sub_diagnostic_setting_deletions[sub_id] = resource_ds_map
    
    confirmed = confirm_uninstall(sub_to_rg_deletions, 
                                  sub_id_to_name, 
                                  role_assignment_deletions, 
                                  sub_diagnostic_setting_deletions)
    if not confirmed:
        log.info("Exiting.")
        raise SystemExit(0)
    
    for sub_id, role_assignment_deletions in role_assignment_deletions.items():
        delete_role_assignments(sub_id, role_assignment_deletions)
    
    for sub_id, diagnostic_setting_deletions in sub_diagnostic_setting_deletions.items():
        delete_diagnostic_settings(sub_id, diagnostic_setting_deletions)
    
    for sub_id, rg_list in sub_to_rg_deletions.items():
        delete_resource_group(sub_id, rg_list)
            
    log.info("Uninstall done! Exiting.")

if __name__ == "__main__":
    basicConfig(level=INFO)
    run(main())