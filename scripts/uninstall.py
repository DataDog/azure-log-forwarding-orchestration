#!/usr/bin/env python
# Remove DataDog Log Forwarding Orchestration from an Azure environment

from asyncio import gather, run
from logging import INFO, WARNING, basicConfig, getLogger
from subprocess import Popen, PIPE
from sys import argv
from typing import Any
import argparse
import json 
import subprocess

# 3p
# requires `pip install azure-mgmt-resource`
# from tenacity import retry, stop_after_attempt

getLogger("azure").setLevel(WARNING)
log = getLogger("uninstaller")

DRY_RUN = True # altan - set to false when done testing
CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX = "lfostorage"
CONTROL_PLANE_CONTAINER_NAME = "control-plane-cache"
RESOURCES_BLOB_NAME = "resources.json"

# ===== Command Execution ===== # 
def pwsh(cmd: str) -> str:
    """
    Run a PowerShell command using subprocess.
    """
    try:
        result = subprocess.run(["pwsh", "-Command", cmd], check=True, text=True, capture_output=True)

        print(result.stdout)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(e.stderr)
        return e.stderr

def az(cmd: str) -> str:
    """Runs the command and returns the stdout, stripping any newlines"""
    
    azCmd = f"az {cmd}"
    
    try:
        result = subprocess.run(azCmd, shell=True, check=True, text=True, capture_output=True)
        # print(f"Azure CLI Output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Error running Azure CLI command:\n{e.stderr}")

    return result.stdout

# ===== Azure Utility ===== #
def setSubscriptionScope(subId: str):
    az(f"account set --subscription {subId}")

def getControlPlaneStorageAccountName(lfoResourceGroupName: str) -> str:
    log.info("Finding storage account with resource cache")
    
    cmd = f"storage account list --resource-group {lfoResourceGroupName} --query \"[?starts_with(name,'{CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX}')]\" --output json"
    storageAccountsJson = json.loads(az(cmd))
    accountName = storageAccountsJson[0]["name"] # altan - assume one account for now but technically could be multiple here
    return accountName

def getResourcesCacheJson(storageAccountName: str) -> dict:
    log.info("Downloading resource cache")
    
    # download resources.json to cloud shell local storage
    resourcesCopy = "resourceCache.json"
    az(f"storage blob download -f ./{resourcesCopy} --account-name {storageAccountName} -c {CONTROL_PLANE_CONTAINER_NAME} -n {RESOURCES_BLOB_NAME}")
    
    log.info("Reading cache to discover tracked resources")
    
    resourcesJson = {}
    with open(resourcesCopy, "r") as file:
        resourcesJson = json.load(file)

    return resourcesJson

def deleteRoleAssignments(accountName: str):
    controlPlaneId = accountName[len(CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX):]
    
    servicePrincipalFilter =  f'''
    displayname eq 'scaling-task-{controlPlaneId}' or 
    displayname eq 'diagnostic-settings-task-{controlPlaneId}' or
    displayname eq 'resources-task-{controlPlaneId}' or
    displayname eq 'deployer-task-{controlPlaneId}'
    '''

    lfoIdentitiesJson = json.loads(az(f"ad sp list --filter \"{servicePrincipalFilter}\""))
    roleDict = {lfoIdentity["appId"]: lfoIdentity["displayName"] for lfoIdentity in lfoIdentitiesJson}    
    roleSummary = dictNewlineSpaced(roleDict)

    roleDeletionLog = f"Deleting all role assignments for following principals:{roleSummary}"
    if DRY_RUN:
        log.info(dryRunOf(roleDeletionLog))
        return
    
    print(roleDeletionLog)
    # for id in principalIds:
    #     az(f"role assignment delete --assignee {id}")

def deleteUnknownRoleAsignments():
    unknownsDeletionLog = "Deleting all Unknown role assignments"
    
    log.info(unknownsDeletionLog)

    if DRY_RUN:
        log.info(dryRunOf(unknownsDeletionLog))
        return
    
    pwsh("Get-AzRoleAssignment | where-object {$_.ObjectType -eq 'Unknown'} | Remove-AzRoleAssignment")
    

def deleteResourceGroup(subId: str, resourceGroupName: str):
    rgDeletionLog = f"Deleting resource group {resourceGroupName}"
    if DRY_RUN:
        log.info(dryRunOf(rgDeletionLog))
        return
    
    log.info(rgDeletionLog)
    az(f"group delete --name {resourceGroupName} --subscription {subId} --yes")
    

def deleteDiagnosticSettings(subId: str, resourceIds: set):
    for resourceId in resourceIds:
        log.info(f"Looking for diagnostic settings to delete for resource {resourceId}")
        
        dsJson = json.loads(az(f"monitor diagnostic-settings list --resource {resourceId} --query \"[?starts_with(name,'datadog_log_forwarding')]\""))
        
        for ds in dsJson:
            dsName = ds["name"]
            dsDeletionLog = f"Deleting diagnostic setting {dsName}"
            if DRY_RUN:
                log.info(f"{dryRunOf(dsDeletionLog)}")
                return

            log.info(dsDeletionLog)
            az(f"monitor diagnostic-settings delete --name {dsName} --resource {resourceId} --subscription {subId}")
            # ResourceNotFoundError: The Resource '<resource-id>' was not found within subscription '<current-subscription-id>'.

def parseResourceIds(resourcesJson):
    resourceIds = set() 
    for _, regionDict in resourcesJson.items():
        for resourceId in regionDict.values():
            resourceIds.update(resourceId)

    return resourceIds

# ===== String Utility ===== #
def commaSeparatedAndQuoted(set: set) -> str:
    return ", ".join(f"\"{i}\"" for i in set)

def setNewlineSpaced(set: set) -> str:
    formatted = "\n".join(f"\t{item}" for item in set)
    return f"\n{formatted}\n"

def dictNewlineSpaced(dict: dict) -> str:
    keys = dict.keys()
    formatted = "\n".join(f"\t{key}: {dict[key]}" for key in keys)
    return f"\n{formatted}\n"

def dryRunOf(s: str) -> str:
    msg = s[0].lower() + s[1:]
    return f"DRY RUN | Would be {msg}"

async def main():
    # altan - Parse out subscription ID and resource group name from script invocation? 
    # altan - Provide dry-run CLI flag 

    # sub_id = input("Enter the subscription ID that holds the control plane")
    # lfoResourceGroupName = input("Enter the resource group name that holds the control plane")

    subId = "0b62a232-b8db-4380-9da6-640f7272ed6d"
    lfoResourceGroupName = "altan-test"

    setSubscriptionScope(subId)

    # altan - verify these things exist
    accountName = getControlPlaneStorageAccountName(lfoResourceGroupName)
    

    resourcesJson = getResourcesCacheJson(accountName)

    monitoredSubIds = set(resourcesJson.keys())
    log.info(f"Looking for artifacts to uninstall in following subscriptions: {setNewlineSpaced(monitoredSubIds)}")

    resourceIds = parseResourceIds(resourcesJson)

    # deleteRoleAssignments(accountName)
    deleteUnknownRoleAsignments()

    # for subId in monitoredSubIds:
    #     log.info(f"Looking for resource group and diagnostic settings to delete in subscription {subId}")
    #     deleteResourceGroup(subId, lfoResourceGroupName)
    #     deleteDiagnosticSettings(subId, resourceIds) #can pass JSON blob into here and parse resources out based on sub ID 


if __name__ == "__main__":
    basicConfig(level=INFO)
    DRY_RUN = True #"--dry-run" in argv
    if DRY_RUN:
        log.info("Dry run enabled, no changes will be made")
    run(main())
    


# Hardcoded for now - these need to be filled in
# RESOURCE_GROUP_NAME="altan-del-test"
# CONTROL_PLANE_SUBSCRIPTION_ID="0b62a232-b8db-4380-9da6-640f7272ed6d"
# MONITORED_SUBSCRIPTIONS=("0b62a232-b8db-4380-9da6-640f7272ed6d")

# # Delete control plane resource group
# echo "Deleting control plane resource group $RESOURCE_GROUP_NAME from subscription $CONTROL_PLANE_SUBSCRIPTION_ID..."
# az group delete --name $RESOURCE_GROUP_NAME --subscription $CONTROL_PLANE_SUBSCRIPTION_ID --yes #--no-wait
# echo "Done"

# # Delete forwarder resource group from each subscription
# for subId in ${MONITORED_SUBSCRIPTIONS[@]}; do
#     echo "Deleting resource group $RESOURCE_GROUP_NAME from subscription $subId..."
#     az group delete --name $RESOURCE_GROUP_NAME --subscription $subId --yes #--no-wait
# done