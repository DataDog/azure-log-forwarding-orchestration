#!/usr/bin/env python
# Remove DataDog Log Forwarding Orchestration from an Azure environment

from asyncio import run
from logging import INFO, WARNING, basicConfig, getLogger
from typing import Any
import argparse
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
def choose_subscription() -> str:
    choice = input("Would you like to search another subscription? (y/n)")
    while choice.lower().strip() not in ["y", "n"]:
        choice = input("Please enter 'y' or 'n'")

    if choice == "n":
        log.info("Exiting.")            
        sys.exit()
    
    log.info("Detecting all subscriptions accessible by the current user")
    subIdNameMap = list_all_subscriptions()
    log.info(f"Found the following subscriptions: {dict_newline_spaced(subIdNameMap)}")
    subId = input("Enter the subscription ID to search for DataDog log forwarding: ")
    while subId.strip() not in subIdNameMap.keys():
        subId = input("Please enter a valid subscription ID from the list above: ")

    return subId
    

def choose_group_to_delete(resourceGroupNames: set) -> str:
    log.info(f"Detected log forwarding installation in the following resource groups: {set_newline_spaced(resourceGroupNames)}")
    
    choice = input("Re-enter the resource group name to confirm uninstallation of the log forwarding instance. The resource group and everything within will be deleted: " )
    
    while choice.strip() not in resourceGroupNames:
        choice = input("Please choose a valid resource group name from the list above: ")

    return choice


def confirm_uninstall(resourceGroupName: str) -> bool:
    log.info(f"Detected log forwarding installation in resource group {resourceGroupName}. Resource group '{resourceGroupName}' and everything within will be deleted.")

    choice = input("Continue? (y/n): ")

    while choice.lower().strip() not in ["y", "n"]:
        choice = input("Please enter 'y' or 'n'")

    return choice == 'y'

# ===== Command Execution ===== # 
def pwsh(cmd: str) -> str:
    """Run PowerShell command, returns stdout"""
    
    pwshCmd = f"pwsh -Command {cmd}"

    try:
        result = subprocess.run(pwshCmd, check=True, text=True, capture_output=True)
        #print(result.stdout)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(e.stderr)
        return e.stderr

def az(cmd: str) -> str:
    """Runs az CLI command and returns stdout"""
    
    azCmd = f"az {cmd}"
    
    try:
        result = subprocess.run(azCmd, shell=True, check=True, text=True, capture_output=True)
        return result.stdout
        # print(f"Azure CLI Output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Error running Azure CLI command:\n{e.stderr}")
        return e.stderr

# ===== Azure Commands ===== #
def get_subscription_info() -> tuple[str, str]:
    subJson = json.loads(az("account show --output json"))
    return subJson["id"], subJson["name"]

def set_subscription_scope(subId: str):
    az(f"account set --subscription {subId}")

def list_all_subscriptions() -> dict:
    allSubsJson = json.loads(az("account list --output json"))
    return {sub["id"]: sub["name"] for sub in allSubsJson}

def find_control_planes() -> dict:
    """Queries for all LFO control planes in a subscription, returns mapping of resource group to control plane storage account name"""
    
    cmd = f"storage account list --query \"[?starts_with(name,'{CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX}')]\" --output json"
    storageAccountsJson = json.loads(az(cmd))

    lfoInstallMap = {account["resourceGroup"] : account["name"] for account in storageAccountsJson} 
    return lfoInstallMap

# altan - need to specify resource group name for proper permissions? 
# def getControlPlaneStorageAccountName(lfoResourceGroupName: str) -> str:
#     log.info("Finding storage account with resource cache")
    
#     cmd = f"storage account list --resource-group {lfoResourceGroupName} --query \"[?starts_with(name,'{CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX}')]\" --output json"
#     storageAccountsJson = json.loads(az(cmd))
#     accountName = storageAccountsJson[0]["name"] # altan - assume one account for now but technically could be multiple here
#     return accountName


def get_resources_cache_json(storageAccountName: str) -> dict:
    log.info("Downloading resource cache")
    
    resourcesCopy = "resourceCache.json"
    az(f"storage blob download --auth-mode login -f ./{resourcesCopy} --account-name {storageAccountName} -c {CONTROL_PLANE_CONTAINER_NAME} -n {RESOURCES_BLOB_NAME}")
    
    log.info("Reading cache to discover tracked resources")
    
    resourcesJson = {}
    with open(resourcesCopy, "r") as file:
        resourcesJson = json.load(file)

    return resourcesJson


def delete_role_assignments(accountName: str):
    controlPlaneId = accountName[len(CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX):]
    
    servicePrincipalFilter =  f'''
    displayname eq 'scaling-task-{controlPlaneId}' or 
    displayname eq 'diagnostic-settings-task-{controlPlaneId}' or
    displayname eq 'resources-task-{controlPlaneId}' or
    displayname eq 'deployer-task-{controlPlaneId}'
    '''

    lfoIdentitiesJson = json.loads(az(f"ad sp list --filter \"{servicePrincipalFilter}\""))
    roleDict = {lfoIdentity["appId"]: lfoIdentity["displayName"] for lfoIdentity in lfoIdentitiesJson}    
    roleSummary = dict_newline_spaced(roleDict)

    roleDeletionLog = f"Deleting all role assignments for following principals:{roleSummary}"
    if DRY_RUN:
        log.info(dry_run_of(roleDeletionLog))
        return
    
    print(roleDeletionLog)
    for id in roleDict.keys():
        az(f"role assignment delete --assignee {id}")


def delete_unknown_role_assignments():
    unknownsDeletionLog = "Deleting all 'Unknown' role assignments"
    
    log.info(unknownsDeletionLog)

    if DRY_RUN:
        log.info(dry_run_of(unknownsDeletionLog))
        return
    
    pwsh("Get-AzRoleAssignment | where-object {$_.ObjectType -eq 'Unknown'} | Remove-AzRoleAssignment")
    

def delete_diagnostic_settings(subId: str, resourcesJson: dict):
    resourceIds = parse_resource_ids(resourcesJson)
    for resourceId in resourceIds:
        log.info(f"Looking for diagnostic settings to delete for resource {resourceId}")
        
        dsJson = json.loads(az(f"monitor diagnostic-settings list --resource {resourceId} --query \"[?starts_with(name,'datadog_log_forwarding')]\""))
        
        for ds in dsJson:
            dsName = ds["name"]
            dsDeletionLog = f"Deleting diagnostic setting {dsName}"
            if DRY_RUN:
                log.info(f"{dry_run_of(dsDeletionLog)}")
                continue

            log.info(dsDeletionLog)
            az(f"monitor diagnostic-settings delete --name {dsName} --resource {resourceId} --subscription {subId}")
            # ResourceNotFoundError: The Resource '<resource-id>' was not found within subscription '<current-subscription-id>'.

def delete_log_forwarder(subId: str, resourceGroupName: str):
    rgDeletionLog = f"Deleting log forwarder resource group {resourceGroupName}"
    if DRY_RUN:
        log.info(dry_run_of(rgDeletionLog))
        return
    
    log.info(rgDeletionLog)
    az(f"group delete --name {resourceGroupName} --subscription {subId} --yes")

def parse_resource_ids(resourcesJson: dict) -> set:
    resourceIds = set() 
    for _, regionDict in resourcesJson.items():
        for resourceId in regionDict.values():
            resourceIds.update(resourceId)

    return resourceIds

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

    # subId, subName = getSubscriptionInfo()
    
    # log.info(f"Searching for log forwarding installs in default subscription {subName} ({subId})")
    resourceGroupToStorageMap = {} #findControlPlanes()
    while not resourceGroupToStorageMap:
        log.info("No log forwarding installs found.")
        subId = choose_subscription()
        set_subscription_scope(subId)
        resourceGroupToStorageMap = find_control_planes()
    
    rgNames = set(resourceGroupToStorageMap.keys())
    lfoResourceGroupName = ""
    if len(rgNames) == 1:
        lfoResourceGroupName = rgNames.pop()
        willContinue = confirm_uninstall(lfoResourceGroupName)
        if not willContinue:
            log.info("Exiting.")
            return
    else:
        lfoResourceGroupName = choose_group_to_delete(rgNames)
    
    lfoStorageAccountName = resourceGroupToStorageMap[lfoResourceGroupName]

    resourcesJson = get_resources_cache_json(lfoStorageAccountName)

    monitoredSubIds = set(resourcesJson.keys())
    
    delete_role_assignments(lfoStorageAccountName)
    delete_unknown_role_assignments()

    log.info(f"Deleting log forwarders in the following monitored subscriptions: {set_newline_spaced(monitoredSubIds)}")

    for subId in monitoredSubIds:
        log.info(f"Looking for diagnostic settings and log forwarders to delete in subscription {subId}")
        delete_diagnostic_settings(subId, resourcesJson)
        delete_log_forwarder(subId, lfoResourceGroupName)

    log.info("Done!")

if __name__ == "__main__":
    basicConfig(level=INFO)
    DRY_RUN = True #"--dry-run" in argv
    if DRY_RUN:
        log.info("Dry run enabled, no changes will be made")
    run(main())