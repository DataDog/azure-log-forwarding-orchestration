#!/usr/bin/env python
# Remove DataDog Log Forwarding Orchestration from an Azure environment

from asyncio import run
from logging import INFO, WARNING, basicConfig, getLogger
import json 
import subprocess
import sys

# 3p
# requires `pip install azure-mgmt-resource`
# from tenacity import retry, stop_after_attempt

getLogger("azure").setLevel(WARNING)
log = getLogger("uninstaller")

DRY_RUN = True # altan - set to false when done testing
CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX = "lfostorage"
CONTROL_PLANE_CONTAINER_NAME = "control-plane-cache"
RESOURCES_BLOB_NAME = "resources.json"

# ===== User Interaction =====  #
def choose_subscription() -> tuple[str, str]:
    choice = input("Would you like to search another subscription? (y/n)")
    while choice.lower().strip() not in ["y", "n"]:
        choice = input("Please enter 'y' or 'n'")

    if choice == "n":
        log.info("Exiting.")            
        sys.exit()
    
    log.info("Detecting all subscriptions accessible by the current user")
    sub_id_name_map = list_all_subscriptions()
    log.info(f"Found the following subscriptions: {dict_newline_spaced(sub_id_name_map)}")
    sub_id = input("Enter the subscription ID to search for DataDog log forwarding: ")
    while sub_id.strip() not in sub_id_name_map.keys():
        sub_id = input("Please enter a valid subscription ID from the list above: ")

    return sub_id, sub_id_name_map[sub_id]
    

def choose_group_to_delete(resource_group_names: set) -> str:
    log.info(f"Detected log forwarding installation in the following resource groups: {set_newline_spaced(resource_group_names)}")
    
    choice = input("Re-enter the resource group name to confirm uninstallation of the log forwarding instance. The resource group and everything within will be deleted: " )
    
    while choice.strip() not in resource_group_names:
        choice = input("Please choose a valid resource group name from the list above: ")

    return choice


def confirm_uninstall(resource_group_name: str) -> bool:
    log.info(f"Detected log forwarding installation in resource group {resource_group_name}. Resource group '{resource_group_name}' and everything within will be deleted.")

    choice = input("Continue? (y/n): ")

    while choice.lower().strip() not in ["y", "n"]:
        choice = input("Please enter 'y' or 'n'")

    return choice == 'y'

# ===== Command Execution ===== # 
def pwsh(cmd: str) -> str:
    """Run PowerShell command, returns stdout"""
    
    pwsh_cmd = f"pwsh -Command {cmd}"

    try:
        result = subprocess.run(pwsh_cmd, check=True, text=True, capture_output=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(e.stderr)
        return e.stderr

def az(cmd: str) -> str:
    """Runs az CLI command and returns stdout"""
    
    az_cmd = f"az {cmd}"
    
    try:
        result = subprocess.run(az_cmd, shell=True, check=True, text=True, capture_output=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running Azure CLI command:\n{e.stderr}")
        return e.stderr

# ===== Azure Commands ===== #
def get_subscription_info() -> tuple[str, str]:
    sub_json = json.loads(az("account show --output json"))
    return sub_json["id"], sub_json["name"]

def set_subscription_scope(sub_id: str):
    az(f"account set --subscription {sub_id}")

def list_all_subscriptions() -> dict:
    all_subs_json = json.loads(az("account list --output json"))
    return {sub["id"]: sub["name"] for sub in all_subs_json}

def find_control_planes(sub_id: str, sub_name: str) -> dict:
    """Queries for all LFO control planes in currently scoped subscription, returns mapping of resource group name to control plane storage account name"""
    
    log.info(f"Searching for log forwarding install in subscription '{sub_name}' ({sub_id})")
    cmd = f"storage account list --query \"[?starts_with(name,'{CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX}')]\" --output json"
    storage_accounts_json = json.loads(az(cmd))

    lfo_install_map = {account["resourceGroup"] : account["name"] for account in storage_accounts_json} 
    return lfo_install_map

# altan - need to specify resource group name for proper permissions? 
# def getControlPlaneStorageAccountName(lfoResourceGroupName: str) -> str:
#     log.info("Finding storage account with resource cache")
    
#     cmd = f"storage account list --resource-group {lfoResourceGroupName} --query \"[?starts_with(name,'{CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX}')]\" --output json"
#     storageAccountsJson = json.loads(az(cmd))
#     accountName = storageAccountsJson[0]["name"] # altan - assume one account for now but technically could be multiple here
#     return accountName


def get_resources_cache_json(storage_account_name: str) -> dict:
    log.info("Downloading resource cache")
    
    resources_copy = "resourceCache.json"
    az(f"storage blob download --auth-mode login -f ./{resources_copy} --account-name {storage_account_name} -c {CONTROL_PLANE_CONTAINER_NAME} -n {RESOURCES_BLOB_NAME}")
    
    log.info("Reading cache to discover tracked resources")
    
    resources_json = {}
    with open(resources_copy, "r") as file:
        resources_json = json.load(file)

    return resources_json


def delete_role_assignments(account_name: str):
    control_plane_id = account_name[len(CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX):]
    
    service_principal_filter =  f'''
    displayname eq 'scaling-task-{control_plane_id}' or 
    displayname eq 'diagnostic-settings-task-{control_plane_id}' or
    displayname eq 'resources-task-{control_plane_id}' or
    displayname eq 'deployer-task-{control_plane_id}'
    '''

    lfo_identities_json = json.loads(az(f"ad sp list --filter \"{service_principal_filter}\""))
    role_dict = {lfoIdentity["appId"]: lfoIdentity["displayName"] for lfoIdentity in lfo_identities_json}    
    role_summary = dict_newline_spaced(role_dict)

    role_deletion_log = f"Deleting all role assignments for following principals:{role_summary}"
    if DRY_RUN:
        log.info(dry_run_of(role_deletion_log))
        return
    
    print(role_deletion_log)
    for id in role_dict.keys():
        az(f"role assignment delete --assignee {id}")


def delete_unknown_role_assignments():
    unknowns_deletion_log = "Deleting all 'Unknown' role assignments"
    
    log.info(unknowns_deletion_log)

    if DRY_RUN:
        log.info(dry_run_of(unknowns_deletion_log))
        return
    
    pwsh("Get-AzRoleAssignment | where-object {$_.ObjectType -eq 'Unknown'} | Remove-AzRoleAssignment")
    

def delete_diagnostic_settings(sub_id: str, resources_json: dict):
    resource_ids = parse_resource_ids(resources_json)
    for resource_id in resource_ids:
        log.info(f"Looking for diagnostic settings to delete for resource {resource_id}")
        
        ds_json = json.loads(az(f"monitor diagnostic-settings list --resource {resource_id} --query \"[?starts_with(name,'datadog_log_forwarding')]\""))
        
        for ds in ds_json:
            ds_name = ds["name"]
            ds_deletion_log = f"Deleting diagnostic setting {ds_name}"
            if DRY_RUN:
                log.info(f"{dry_run_of(ds_deletion_log)}")
                continue

            log.info(ds_deletion_log)
            az(f"monitor diagnostic-settings delete --name {ds_name} --resource {resource_id} --subscription {sub_id}")
            # ResourceNotFoundError: The Resource '<resource-id>' was not found within subscription '<current-subscription-id>'.

def delete_log_forwarder(sub_id: str, resource_group_name: str):
    rg_deletion_log = f"Deleting log forwarder resource group {resource_group_name}"
    if DRY_RUN:
        log.info(dry_run_of(rg_deletion_log))
        return
    
    log.info(rg_deletion_log)
    az(f"group delete --name {resource_group_name} --subscription {sub_id} --yes")

def parse_resource_ids(resources_json: dict) -> set:
    resource_ids = set() 
    for _, region_dict in resources_json.items():
        for resource_id in region_dict.values():
            resource_ids.update(resource_id)

    return resource_ids

# ===== String Utility ===== #
def dry_run_of(s: str) -> str:
    msg = s[0].lower() + s[1:]
    return f"DRY RUN | Would be {msg}"

def comma_separated_and_quoted(set: set) -> str:
    return ", ".join(f"\"{i}\"" for i in set)

def set_newline_spaced(set: set) -> str:
    formatted = "\n".join(f"\t{item}" for item in set)
    return f"\n{formatted}\n"

def dict_newline_spaced(dict: dict) -> str:
    keys = dict.keys()
    formatted = "\n".join(f"\t{key} | {dict[key]}" for key in keys)
    return f"\n{formatted}\n"

async def main():
    # altan - Provide dry-run CLI flag 

    sub_id, sub_name = get_subscription_info()
    
    resource_group_storage_map = find_control_planes(sub_id, sub_name)
    while not resource_group_storage_map:
        log.info(f"No log forwarding installs found in subscription '{sub_name} ({sub_id})'.")
        sub_id, sub_name = choose_subscription()
        set_subscription_scope(sub_id)
        resource_group_storage_map = find_control_planes(sub_id, sub_name)
    
    rg_names = set(resource_group_storage_map.keys())
    lfo_resource_group_name = ""
    if len(rg_names) == 1:
        lfo_resource_group_name = rg_names.pop()
        will_continue = confirm_uninstall(lfo_resource_group_name)
        if not will_continue:
            log.info("Exiting.")
            return
    else:
        lfo_resource_group_name = choose_group_to_delete(rg_names)
    
    lfo_storage_account_name = resource_group_storage_map[lfo_resource_group_name]
    resources_json = get_resources_cache_json(lfo_storage_account_name)
    monitored_sub_ids = set(resources_json.keys())
    
    delete_role_assignments(lfo_storage_account_name)
    delete_unknown_role_assignments()

    log.info(f"Deleting log forwarders in the following monitored subscriptions: {set_newline_spaced(monitored_sub_ids)}")

    for sub_id in monitored_sub_ids:
        log.info(f"Looking for diagnostic settings and log forwarders to delete in subscription {sub_id}")
        delete_diagnostic_settings(sub_id, resources_json)
        delete_log_forwarder(sub_id, lfo_resource_group_name)

    log.info("Done!")

if __name__ == "__main__":
    basicConfig(level=INFO)
    DRY_RUN = True #"--dry-run" in argv
    if DRY_RUN:
        log.info("Dry run enabled, no changes will be made")
    run(main())