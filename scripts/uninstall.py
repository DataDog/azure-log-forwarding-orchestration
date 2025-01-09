#!/usr/bin/env python
# Remove DataDog Log Forwarding Orchestration from an Azure environment
# Last updated: Jan 2025
# User running this script will need the following permissions: 

from asyncio import run
from collections import defaultdict
from logging import INFO, WARNING, basicConfig, getLogger
from typing import Any, Iterable, Final
import argparse
import json 
import subprocess

# 3p
# from tenacity import retry, stop_after_attempt

getLogger("azure").setLevel(WARNING)
log = getLogger("uninstaller")

DRY_RUN = False

# ===== Constants ===== # 
CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX: Final = "lfostorage"
DIAGNOSTIC_SETTING_PREFIX: Final = "datadog_log_forwarding_"

ALLOWED_TYPES_PER_PROVIDER = {
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

ALLOWED_RESOURCE_TYPES = [
    f"{rp}/{rt}".casefold() for rp, resource_types in ALLOWED_TYPES_PER_PROVIDER.items() for rt in resource_types
]

ALLOWED_RESOURCE_TYPES_FILTER = " || ".join([f"type == '{rt}'" for rt in ALLOWED_RESOURCE_TYPES])

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

# ===== Azure Commands ===== #
def get_users_subscriptions() -> dict:
    log.info("Searching for all subscriptions accessible by the current user to find log forwarding installations")
    
    all_subs_json = json.loads(az("account list --output json"))
    return {sub["id"]: sub["name"] for sub in all_subs_json}

def list_resources(sub_id: str) -> set:
    resource_ids = json.loads(az(f"resource list --subscription {sub_id} --query \"[?{ALLOWED_RESOURCE_TYPES_FILTER}].id\" --output json"))
    return set(resource_ids)

def find_control_planes(sub_id: str, sub_name: str) -> dict[str,str]:
    """Queries for LFO control planes in single subscription, returns mapping of resource group name to control plane storage account name"""
    
    log.info(f"Searching for Datadog log forwarding instance in subscription '{sub_name}' ({sub_id})")
    cmd = f"storage account list --subscription {sub_id} --query \"[?starts_with(name,'{CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX}')].{{resourceGroup:resourceGroup, name:name}}\" --output json"
    storage_accounts_json = json.loads(az(cmd))

    lfo_install_map = {account["resourceGroup"] : account["name"] for account in storage_accounts_json} 
    return lfo_install_map

def find_lfo_control_planes(sub_id_to_name: dict) -> tuple[dict[str,list[str]], dict[str,list[str]]]:
    """
    Queries for all LFO control planes that the user has access to.
    Returns 2 dictionaries - a subcription ID to resource group mapping and a resource group to storage account mapping
    """

    sub_to_rg = defaultdict(list)
    rg_to_storage = defaultdict(list)

    for sub_id, sub_name in sub_id_to_name.items():
        control_planes = find_control_planes(sub_id, sub_name)
        if not control_planes:
            continue

        for resource_group_name, storage_account_name in control_planes.items():        
            sub_to_rg[sub_id].append(resource_group_name)
            rg_to_storage[resource_group_name].append(storage_account_name)
        
    return sub_to_rg, rg_to_storage

def find_role_assignments(sub_id: str, sub_name: str, control_plane_ids: set) -> Any:
    log.info(f"Looking for DataDog diagnostic settings to delete in {sub_name} ({sub_id})")
    
    description_filter = " || ".join(f"description == 'ddlfo{id}'" for id in control_plane_ids)
    return json.loads(az(f"role assignment list --all --subscription {sub_id} --query \"[?description != null && ({description_filter})].{{id: id, roleDefinitionName: roleDefinitionName, principalId: principalId}}\" --output json"))

def delete_role_assignments(sub_id: str, role_assigments_json: Any):
    ids = {role["id"] for role in role_assigments_json}
    if not ids:
        log.info(f"Did not find any role assignments to delete in subscription {sub_id}")
        return 
    
    role_infos = [f"Role: {role['roleDefinitionName']}, Principal: {role['principalId']}" for role in role_assigments_json]
    role_deletion_log = f"Deleting role assignments from subscription {sub_id}:\n{indented_log_of(role_infos)}"

    if DRY_RUN:
        log.info(dry_run_of(role_deletion_log))
        return

    # --include-inherited flag will also try to delete the role assignment from the management group scope if possible
    log.info(role_deletion_log)
    az(f"role assignment delete --ids {space_separated(ids)} --subscription {sub_id} --include-inherited --yes")

def find_diagnostic_settings(sub_id: str, sub_name: str, control_plane_ids: set) -> dict[str, list[str]]:
    log.info(f"Looking for DataDog diagnostic settings to delete in {sub_name} ({sub_id})")
    
    resource_ids = list_resources(sub_id)
    resource_ds_map = defaultdict(list)     
    
    diagnostic_settings_filter = " || ".join(f"name == '{DIAGNOSTIC_SETTING_PREFIX}{id}'" for id in control_plane_ids)
    for resource_id in resource_ids:        
        ds_names = json.loads(az(f"monitor diagnostic-settings list --resource {resource_id} --query \"[?{diagnostic_settings_filter}].name\""))
        for ds_name in ds_names:
            resource_ds_map[resource_id].append(ds_name)
    
    return resource_ds_map
            
def delete_diagnostic_settings(sub_id: str, resource_setting_map: dict[str,list[str]]):    
    ds_deletion_log = f"Deleting DataDog diagnostic settings from {len(resource_setting_map)} resources in subscription {sub_id}"
    if DRY_RUN:
        log.info(f"{dry_run_of(ds_deletion_log)}")
        return

    log.info(ds_deletion_log)
    for resource_id, ds_names in resource_setting_map.items():
        for ds_name in ds_names:
            az(f"monitor diagnostic-settings delete --name {ds_name} --resource {resource_id} --subscription {sub_id}")

# ===== Resource Group Delete ===== #
def delete_resource_group(sub_id: str, resource_group_list: list[str]):
    for resource_group in resource_group_list:
        rg_deletion_log = f"Deleting resource group {resource_group}"
        if DRY_RUN:
            log.info(dry_run_of(rg_deletion_log))
            return
        
        log.info(rg_deletion_log)
        az(f"group delete --subscription {sub_id} --name {resource_group} --yes")

# ===== Dict Utility ===== #
def firstKeyOf(d: dict[str,Any]) -> str:
    return next(iter(d))

# ===== String Utility ===== #
def dry_run_of(s: str) -> str:
    msg = s[0].lower() + s[1:]
    return f"DRY RUN | Would be {msg}"

def space_separated(iter: Iterable) -> str:
    return " ".join(iter)

def comma_separated_quoted(set: set) -> str:
    return ", ".join(f"\"{i}\"" for i in set)

def indented_log_of(iter: Iterable) -> str:
    formatted = "\n".join(f"\t{item}" for item in iter)
    return f"\n{formatted}\n"

def dict_newline_spaced(dict: dict) -> str:
    items = dict.items()
    formatted = "\n".join(f"\t{key} | {value}" for key, value in items)
    return f"\n{formatted}\n"

def uninstall_summary(sub_to_rg_deletions: dict[str,list[str]], 
                      sub_id_to_name: dict[str,str],
                      role_assignment_deletions: dict[str, Iterable],
                      resource_ds_map: dict[str, list[str]]) -> str:
    deletionSummary = "The following role assignments, diagnostic settings, and resource groups will be deleted as part of the uninstallation process.\n"
    
    for sub_id, rg_list in sub_to_rg_deletions.items():
        role_assignments = role_assignment_deletions[sub_id]
        deletionSummary += f"=== From subscription {sub_id_to_name[sub_id]} ({sub_id}) === \n"
        deletionSummary += "Role Assignments:\n"
        deletionSummary += f"{indented_log_of(f"Role Assignment {role_assignment["id"]} ({role_assignment["roleDefinitionName"]} with principal {role_assignment["principalId"]})\n" for role_assignment in role_assignments)}"
        deletionSummary += "Diagnostic Settings:\n"
        
        resource_list = resource_ds_map[sub_id]
        num_ds = 0
        for resource_id in resource_list:
            ds_list = resource_ds_map[resource_id]
            num_ds += len(ds_list)
        
        deletionSummary += f"{num_ds} diagnostic settings will be deleted from {len(resource_list)} resources\n"
        
        deletionSummary += "Resource Groups:\n"
        for rg in rg_list:
            deletionSummary += f"Resource Group {rg}\n"
    
    return deletionSummary

# ===== User Interaction =====  #
def confirm_uninstall(sub_to_rg_deletions: dict[str,list[str]], 
                      sub_id_to_name: dict[str,str],
                      role_assignment_deletions: dict[str, Iterable],
                      resource_ds_map: dict[str, list[str]]) -> bool:

    summary = uninstall_summary(sub_to_rg_deletions, sub_id_to_name, role_assignment_deletions, resource_ds_map)
    log.info(summary)
    
    choice = input("Continue? (y/n): ").lower().strip()
    while choice not in ["y", "n"]:
        choice = input("Continue? (y/n): ").lower().strip()

    return choice == 'y'

def choose_resource_groups_to_delete(resource_groups_in_sub: list[str]) -> list[str]:
    """Given list of resource groups, prompt the user to what to delete. Returns list of what was selected"""
    
    prompt = '''
    Enter the resource group name you would like to remove
    To remove all of them, enter *
    To remove nothing, enter -
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
    
    log.info(f"Found log forwarding installation in subscription '{sub_name}' ({sub_id})")
    
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
        sub_id = firstKeyOf(sub_id_to_rgs)
        sub_name = sub_id_to_name[sub_id]
        resource_groups_in_sub = sub_id_to_rgs[sub_id]        
        rgs_to_delete = identify_resource_groups_to_delete(sub_id, sub_name, resource_groups_in_sub)         
        
        sub_id_to_rg_deletions[sub_id] = rgs_to_delete
        return sub_id_to_rg_deletions
    
    log.info("Found multiple subscriptions with log forwarding installations")
    for sub_id, rg_list in sub_id_to_rgs.items():
        sub_name = sub_id_to_name[sub_id]
        rgs_to_delete = identify_resource_groups_to_delete(sub_id, sub_name, rg_list)
        sub_id_to_rg_deletions[sub_id] = rgs_to_delete

    return sub_id_to_rg_deletions

def mark_control_plane_deletions(sub_to_rg_deletions: dict[str, list[str]], rg_to_storage_account: dict[str,list[str]]) -> set[str]:
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
    1) Fetch all subscriptions accessible by the current user. Cache them. 
    2) For each subscription, search for LFO control planes. 
       Cache the subscription to resource group mapping and the control plane to storage account mapping.
    3) For each subscription, determine which resource groups need to be deleted. User input may occur to disambiguate. 
    4) Summarize what resource groups will be deleted and display to user. User input required to confirm that we should proceed.
    5) Based on the resource groups, cache the corresponding control plane IDs. 
    6) Based on subscriptions and control plane IDs, delete all LFO role assignments and diagnostic settings.
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
        log.warning("If you have created any Azure resources in resource groups managed by DataDog log forwarding, they will be deleted. Perform backups if necessary")

    sub_id_to_name = get_users_subscriptions()    
    sub_id_to_rgs, rg_to_storage_account = find_lfo_control_planes(sub_id_to_name)
    sub_to_rg_deletions = mark_rg_deletions_per_sub(sub_id_to_name, sub_id_to_rgs)

    if not sub_to_rg_deletions or sub_to_rg_deletions is None or not any(sub_to_rg_deletions.values()):
        log.info("Could not find any resource groups to delete as part of uninstall process. Exiting.")
        raise SystemExit(0)
    
    control_plane_id_deletions = mark_control_plane_deletions(sub_to_rg_deletions, rg_to_storage_account)    
    role_assignment_deletions = defaultdict(Iterable) 
    sub_diagnostic_setting_deletions = defaultdict(dict) 

    for sub_id, rg_list in sub_to_rg_deletions.items():
        sub_name = sub_id_to_name[sub_id]
        role_assignment_json = find_role_assignments(sub_id, sub_name, control_plane_id_deletions)
        if role_assignment_json:
            role_assignment_deletions[sub_id] = role_assignment_json
        
        resource_ds_map = find_diagnostic_settings(sub_id, sub_name, control_plane_id_deletions)
        if resource_ds_map:
            sub_diagnostic_setting_deletions[sub_id] = resource_ds_map
        
    if DRY_RUN:
        return
    
    confirmed = confirm_uninstall(sub_to_rg_deletions, sub_id_to_name, role_assignment_deletions, resource_ds_map)
    if not confirmed:
        log.info("Exiting.")
        raise SystemExit(0)
    
    for sub_id, role_assignment_deletions in role_assignment_deletions.items():
        delete_role_assignments(sub_id, role_assignment_deletions)
    
    for sub_id, diagnostic_setting_deletions in sub_diagnostic_setting_deletions.items():
        delete_diagnostic_settings(sub_id, diagnostic_setting_deletions)
    
    for sub_id, rg_list in sub_to_rg_deletions.items():
        delete_resource_group(sub_id, rg_list)
            
        
    log.info("Done!")

    # for async: process executor, thread pool executor
    # check for existence of the things we tried to delete. if exists, retry deletion
    # Verify that unknown role assigments will still appear if you're querying by description

if __name__ == "__main__":
    basicConfig(level=INFO)
    run(main())