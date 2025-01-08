#!/usr/bin/env python
# Remove DataDog Log Forwarding Orchestration from an Azure environment
# User running this script will need the following permissions: 


# multiple API calls for diag settings
# what resources do we need to modify?
    # query azure, resource cache could be messed up
# do the deletion
    # idc if it exists or not, delete it. 

from asyncio import run
from logging import INFO, WARNING, basicConfig, getLogger
from typing import Any, Iterable, Final
import argparse
import json 
import subprocess
import sys

# 3p
# from tenacity import retry, stop_after_attempt

getLogger("azure").setLevel(WARNING)
log = getLogger("uninstaller")

DRY_RUN = False

# ===== Constants ===== # 
CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX: Final = "lfostorage"
DIAGNOSTIC_SETTING_PREFIX: Final = "datadog_log_forwarding_"

# ===== Command Execution ===== # 
def az(cmd: str) -> str:
    """Runs az command, returns stdout"""
    
    az_cmd = f"az {cmd}"
    
    try:
        result = subprocess.run(az_cmd, shell=True, check=True, text=True, capture_output=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running Azure CLI command:\n{e.stderr}")
        sys.exit(0)

# ===== Azure Commands ===== #
def set_subscription_scope(sub_id: str):
    az(f"account set --subscription {sub_id}")

def get_users_subscriptions() -> dict:
    all_subs_json = json.loads(az("account list --output json"))
    return {sub["id"]: sub["name"] for sub in all_subs_json}

def list_resources(sub_id: str) -> set:
    resource_ids = json.loads(az(f"resource list --subscription {sub_id} --query \"[].id\" --output json"))
    return set(resource_ids)

def find_control_planes(sub_id: str, sub_name: str) -> dict:
    """Queries for LFO control planes in single subscription, returns mapping of resource group name to control plane storage account name"""
    
    log.info(f"Searching for log forwarding install in subscription '{sub_name}' ({sub_id})")
    cmd = f"storage account list --subscription {sub_id} --query \"[?starts_with(name,'{CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX}')]\" --output json"
    storage_accounts_json = json.loads(az(cmd))

    lfo_install_map = {account["resourceGroup"] : account["name"] for account in storage_accounts_json} 
    return lfo_install_map

def find_lfo_resource_groups(sub_id_to_name: dict) -> tuple[dict[str,list[str]], dict[str,list[str]]]:
    """
    Queries for all LFO control planes that the user has access to
    Returns 2 dictionaries - a subcription ID to resource group mapping and a resource group to storage account mapping
    """

    sub_to_rg = {}
    rg_to_storage = {}

    for sub_id, sub_name in sub_id_to_name.items():
        control_plane = find_control_planes(sub_id, sub_name)
        if not control_plane:
            continue

        if sub_id not in sub_to_rg:
            sub_to_rg[sub_id] = []

        for resource_group_name, storage_account_name in control_plane.items():        
            if resource_group_name not in rg_to_storage:
                rg_to_storage[resource_group_name] = []

            sub_to_rg[sub_id].append(resource_group_name)
            rg_to_storage[resource_group_name].append(storage_account_name)
        
    return sub_to_rg, rg_to_storage

# altan - need to specify resource group name for proper permissions? 
def get_control_plane_storage_account(lfo_resource_group_name: str) -> str:
    log.info("Finding storage account with resource cache")
    
    cmd = f"storage account list --resource-group {lfo_resource_group_name} --query \"[?starts_with(name,'{CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX}')]\" --output json"
    storage_accounts_json = json.loads(az(cmd))
    account_name = storage_accounts_json[0]["name"]
    return account_name

# ===== Delete Behavior ===== #
def delete_role_assignments(sub_id: str, control_plane_ids: set):
    description_filter = " || ".join(f"description == 'ddlfo{id}'" for id in control_plane_ids)
    role_assigments_ids = json.loads(az(f"role assignment list --all --subscription {sub_id} --query \"[?description != null && ({description_filter})].{{id: id, name: name}}\" --output json"))

    role_deletion_log = f"Deleting role assignments from subscription {sub_id}:\n{indented_log_of(role_assigments_ids)}"
    if DRY_RUN:
        log.info(dry_run_of(role_deletion_log))
        return

    # --include-inherited flag will also try to delete the role assignment from the management group scope if possible
    az(f"role assignment delete --ids {space_separated(role_assigments_ids)} --subscription {sub_id} --include-inherited --yes")


def delete_diagnostic_settings(sub_id: str, resource_ids: set, control_plane_id_deletions: set):
    for resource_id in resource_ids:
        log.info(f"Looking for DataDog log forwarding diagnostic settings to delete for resource {resource_id}")
        
        ds_names = json.loads(az(f"monitor diagnostic-settings list --resource {resource_id} --query \"[?starts_with(name,'{DIAGNOSTIC_SETTING_PREFIX}')].name\""))
        
        for ds_name in ds_names:
            ds_control_plane_id = ds_name[len(DIAGNOSTIC_SETTING_PREFIX):]
            if ds_control_plane_id not in control_plane_id_deletions:
                continue

            ds_deletion_log = f"Deleting diagnostic setting {ds_name}"
            if DRY_RUN:
                log.info(f"{dry_run_of(ds_deletion_log)}")
                continue

            log.info(ds_deletion_log)
            az(f"monitor diagnostic-settings delete --name {ds_name} --resource {resource_id} --subscription {sub_id}")

def delete_resource_group(sub_id: str, resource_group_name: str):
    rg_deletion_log = f"Deleting resource group {resource_group_name}"
    if DRY_RUN:
        log.info(dry_run_of(rg_deletion_log))
        return
    
    log.info(rg_deletion_log)
    az(f"group delete --subscription {sub_id} --name {resource_group_name} --yes")

# ===== Dict Utility ===== #
def firstKeyOf(d: dict[str,Any]) -> str:
    return next(iter(d))

# ===== String Utility ===== #
def dry_run_of(s: str) -> str:
    msg = s[0].lower() + s[1:]
    return f"DRY RUN | Would be {msg}"

def space_separated(iter: Iterable) -> str:
    return " ".join(iter)

def comma_separated_and_quoted(set: set) -> str:
    return ", ".join(f"\"{i}\"" for i in set)

def indented_log_of(iter: Iterable) -> str:
    formatted = "\n".join(f"\t{item}" for item in iter)
    return f"\n{formatted}\n"

def dict_newline_spaced(dict: dict) -> str:
    items = dict.items()
    formatted = "\n".join(f"\t{key} | {value}" for key, value in items)
    return f"\n{formatted}\n"

def uninstall_summary_for(sub_id_to_rgs: dict[str, list[str]], sub_id_to_name: dict[str,str]) -> str:
    return "\n".join(f"Subscription '{sub_id_to_name[sub_id]}' ({sub_id}) \n {indented_log_of(rg_list)}" for sub_id, rg_list in sub_id_to_rgs.items())

# ===== User Interaction =====  #
def confirm_uninstall(sub_to_rg_deletions: dict[str,list[str]], sub_id_to_name: dict[str,str]) -> bool:
    confirm_log = f"The following resource groups will be deleted as part of the uninstall process: {uninstall_summary_for(sub_to_rg_deletions, sub_id_to_name)}" 
    log.info(confirm_log)
    
    choice = input("Continue? (y/n): ")
    while choice.lower().strip() not in ["y", "n"]:
        choice = input("Continue? (y/n): ")

    return choice == 'y'

def choose_resource_groups_to_delete(resource_groups_in_sub: list[str]) -> list[str]:
    """Given list of resource groups, prompt the user to choose one to delete or all of them. Returns the list of resource groups selected"""
    
    chosen_rg = input("Copy/paste the resource group name you would like to remove. If you would like to uninstall everything, enter '*': ").strip().lower()

    will_delete_all = chosen_rg == "*"
    if will_delete_all:
        return resource_groups_in_sub

    while chosen_rg not in resource_groups_in_sub:
        chosen_rg = input("Please enter a valid resource group name from the list above or '*' to delete everything: ").strip().lower()
        will_delete_all = chosen_rg == "*"
        if will_delete_all:
            return resource_groups_in_sub

    return [chosen_rg]

def identify_resource_groups_to_delete(sub_id: str, sub_name: str, resource_groups_in_sub: list[str]) -> list[str]:
    """For given subscription, prompt the user to choose which resource group (or all) to delete if there's multiple. Returns a list of resource groups to delete"""
    
    log.info(f"Found log forwarding installation in subscription '{sub_name}' ({sub_id})")
    
    if len(resource_groups_in_sub) == 1:
        log.info(f"Found single resource group with log forwarding artifact: '{resource_groups_in_sub[0]}'")
        return resource_groups_in_sub
    
    log.info(f"Found multiple resource groups with log forwarding artifacts: '{indented_log_of(resource_groups_in_sub)}'")
    log.warning("The following action will lead to the deletion of resource groups. ALL resources within the group will be deleted as well. Please backup any important resources before proceeding.")
    return choose_resource_groups_to_delete(resource_groups_in_sub)

def mark_rg_deletions_per_sub(sub_id_to_name: dict[str,str], 
                              sub_id_to_rg_list: dict[str,list[str]]) -> dict[str, list[str]]:
    """Returns mapping of subscription ID to resource groups within it to delete. May prompt user for input"""
    
    sub_id_to_rg_deletions = {}
    if not sub_id_to_rg_list:
        log.info("Did not find any log forwarding installations")
    elif len(sub_id_to_rg_list) == 1:
        sub_id = firstKeyOf(sub_id_to_rg_list)
        sub_name = sub_id_to_name[sub_id]
        resource_groups_in_sub = sub_id_to_rg_list[sub_id]        
        rgs_to_delete = identify_resource_groups_to_delete(sub_id, sub_name, resource_groups_in_sub)        
        
        sub_id_to_rg_deletions[sub_id] = rgs_to_delete
    elif len(sub_id_to_rg_list) > 1:
        log.info("Found multiple subscriptions with log forwarding installations")
        for sub_id, rg_list in sub_id_to_rg_list.items():
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
    Uninstallation Overview:
    1) Fetch all subscriptions accessible by the current user and cache them. 
    2) Within each subscription, search for the LFO control plane. When found, prompt user to confirm uninstall.
    3) Delete all LFO role assignments that correspond with the LFO being uninstalled.
    4) For each subscription from step 1, discover & delete LFO resource groups + diagnostic settings
    '''

    parser = argparse.ArgumentParser(description="Uninstall DataDog Log Forwarding Orchestration from an Azure environment")
    parser.add_argument("-d", "--dry-run", action="store_true", help="Run the script in dry-run mode, no changes will be made")
    args = parser.parse_args()

    if args.dry_run:
        DRY_RUN = True
        log.info("Dry run enabled, no changes will be made")

    DRY_RUN = True # testing


    log.info("Searching for all subscriptions accessible by the current user to find log forwarding installations")
    sub_id_to_name = get_users_subscriptions()
    
    sub_id_to_rg_list, rg_to_storage_account = find_lfo_resource_groups(sub_id_to_name)
    sub_to_rg_deletions = mark_rg_deletions_per_sub(sub_id_to_name, sub_id_to_rg_list)

    if not sub_to_rg_deletions or sub_to_rg_deletions is None:
        log.info("Could not find any resource groups to delete as part of uninstall process.")
        return
    
    confirmed = confirm_uninstall(sub_to_rg_deletions, sub_id_to_name)
    if not confirmed:
        log.info("Exiting.")
        sys.exit(0)

    control_plane_id_deletions = mark_control_plane_deletions(sub_to_rg_deletions, rg_to_storage_account)
    
    for sub_id, rg_list in sub_to_rg_deletions.items():
        delete_role_assignments(sub_id, control_plane_id_deletions)
        
        resource_ids = list_resources(sub_id)
        delete_diagnostic_settings(sub_id, resource_ids, control_plane_id_deletions)
        
        for resource_group in rg_list:
            delete_resource_group(sub_id, resource_group)
            
        
    log.info("Done!")

    # for async: process executor, thread pool executor
    # check for existence of the things we tried to delete. if exists, retry deletion
    # Verify that unknown role assigments will still appear if you're querying by description

if __name__ == "__main__":
    basicConfig(level=INFO)
    run(main())