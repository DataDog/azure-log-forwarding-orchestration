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

DRY_RUN = True

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
        SystemExit(1)

# ===== Azure Commands ===== #
def set_subscription_scope(sub_id: str):
    az(f"account set --subscription {sub_id}")

def get_users_subscriptions() -> dict:
    log.info("Searching for all subscriptions accessible by the current user to find log forwarding installations")
    
    all_subs_json = json.loads(az("account list --output json"))
    return {sub["id"]: sub["name"] for sub in all_subs_json}

def list_resources(sub_id: str) -> set:
    resource_ids = json.loads(az(f"resource list --subscription {sub_id} --query \"[].id\" --output json"))
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

            ds_deletion_log = f"Deleting diagnostic setting {ds_name} within subscription {sub_id}"
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

def comma_separated_quoted(set: set) -> str:
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
    
    choice = input("Continue? (y/n): ").lower.strip()
    while choice not in ["y", "n"]:
        choice = input("Continue? (y/n): ").lower().strip()

    return choice == 'y'

def choose_resource_groups_to_delete(resource_groups_in_sub: list[str]) -> list[str]:
    """Given list of resource groups, prompt the user to choose one to delete or all of them. Returns the list of resource groups selected"""
    
    chosen_rg = input("Copy/paste the resource group name you would like to remove. If you would like to uninstall everything, enter '*': ").strip().lower()

    while chosen_rg not in resource_groups_in_sub:
        if chosen_rg == "*":
            return resource_groups_in_sub

        chosen_rg = input("Please enter a valid resource group name from the list above or '*' to delete everything: ").strip().lower()        

    return [chosen_rg]

def identify_resource_groups_to_delete(sub_id: str, sub_name: str, resource_groups_in_sub: list[str]) -> list[str]:
    """For given subscription, prompt the user to choose which resource group (or all) to delete if there's multiple. Returns a list of resource groups to delete"""
    
    log.info(f"Found log forwarding installation in subscription '{sub_name}' ({sub_id})")
    
    if len(resource_groups_in_sub) == 1:
        log.info(f"Found single resource group with log forwarding artifact: '{resource_groups_in_sub[0]}'")
        return resource_groups_in_sub
    
    log.info(f"Found multiple resource groups with log forwarding artifacts: '{indented_log_of(resource_groups_in_sub)}'")
    
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

    if not sub_to_rg_deletions or sub_to_rg_deletions is None:
        log.info("Could not find any resource groups to delete as part of uninstall process. Exiting.")
        SystemExit(0)
    
    confirmed = confirm_uninstall(sub_to_rg_deletions, sub_id_to_name)
    if not confirmed:
        log.info("Exiting.")
        SystemExit(0)

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