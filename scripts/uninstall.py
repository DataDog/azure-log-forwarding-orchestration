#!/usr/bin/env python
# Remove DataDog Log Forwarding Orchestration from an Azure environment

from asyncio import gather
from logging import INFO, WARNING, basicConfig, getLogger
# from os import environ
from sys import argv
from typing import Any
import argparse
import json 
import subprocess

# 3p
# requires `pip install azure-mgmt-resource`
# from azure.identity.aio import DefaultAzureCredential
# from azure.mgmt.resource.resources.v2022_09_01.aio import ResourceManagementClient
# from azure.mgmt.resource.resources.v2022_09_01.models import Resource
# from tenacity import retry, stop_after_attempt

getLogger("azure").setLevel(WARNING)
log = getLogger("forwarder_cleanup")

DRY_RUN = True # altan - set to false when done testing
CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX = "lfostorage"
CONTROL_PLANE_CONTAINER_NAME = "control-plane-cache"
RESOURCES_BLOB_NAME = "resources.json"

def executePowershell(cmd: str) -> str:
    """
    Run a PowerShell command using subprocess.
    """
    try:
        result = subprocess.run(["pwsh", "-Command", cmd], check=True, text=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        print(e.stderr)

    return result.stdout

def execute(cmd: str) -> str:
    """Runs the command and returns the stdout, stripping any newlines"""
    try:
        result = subprocess.run(cmd, shell=True, check=True, text=True, capture_output=True)
        print(f"Azure CLI Output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Error running Azure CLI command:\n{e.stderr}")

    return result.stdout

def newlineSpaced(s: str) -> str:
    return f"\n{s}\n"

def dryRunOf(s: str) -> str:
    msg = s[0].lower() + s[1:]
    return f"DRY RUN | Would be {msg}"

def deleteRoleAssignments(controlPlaneId: str):
    servicePrincipalFilter =  f'''
    displayname eq 'scaling-task-{controlPlaneId}' or 
    displayname eq 'diagnostic-settings-task-{controlPlaneId}' or
    displayname eq 'resources-task-{controlPlaneId}' or
    displayname eq 'deployer-task-{controlPlaneId}'
    '''

    lfoIdentitiesJson = json.loads(execute(f"az ad sp list --filter {servicePrincipalFilter}"))
    roleDict = {lfoIdentity["appId"]: lfoIdentity["displayName"] for lfoIdentity in lfoIdentitiesJson}
    principalIds = roleDict.keys()
    
    roleSummary = newlineSpaced("\n".join(f"{id}: {roleDict[id]}" for id in principalIds))
    roleDeletionLog = f"Deleting all role assignments for following principals:{roleSummary}"

    if DRY_RUN:
        print(dryRunOf(roleDeletionLog))
        return
    
    print(roleDeletionLog)
    for id in principalIds:
        execute(f"az role assignment delete --assignee {id}")

def deleteUnknownRoles():
    unknownsDeletionLog = "Deleting all unknown role assignments"
    
    if DRY_RUN:
        print(dryRunOf(unknownsDeletionLog))
        return
    
    print(unknownsDeletionLog)
    script = "Get-AzRoleAssignment | where-object {$_.ObjectType -eq 'Unknown'} | Remove-AzRoleAssignment"
    executePowershell(script)

def deleteResourceGroup(subId: str, resourceGroupName: str):
    rgDeletionLog = f"Deleting resource group {resourceGroupName} in subscription {subId}"
    if DRY_RUN:
        print(dryRunOf(rgDeletionLog))
        return
    
    print(rgDeletionLog)
    execute(f"az group delete --name {resourceGroupName} --subscription {subId} --yes")

def deleteDiagnosticSettings(subId: str, resourceIds: set):
    for resourceId in resourceIds:
        dsJson = json.loads(execute(f"az monitor diagnostic-settings list --resource {resourceId} --query \"[?starts_with(name,'datadog_log_forwarding')]\""))
        dsName = dsJson["name"]
        execute(f"az monitor diagnostic-settings delete --name {dsName} --resource {resourceId} --subscription {subId}")

    # ResourceNotFoundError: The Resource '<resource-id>' was not found within subscription '<current-subscription-id>'.

async def main():
    # altan - Parse out subscription ID and resource group name from script invocation? 
    # altan - Provide dry-run CLI flag 

    controlPlaneSubId = input("Enter the subscription ID that holds the control plane")
    lfoResourceGroupName = input("Enter the resource group name that holds the control plane")

    sub_id = "0b62a232-b8db-4380-9da6-640f7272ed6d"

    # altan - verify these things exist
    
    # sets subscription context
    execute(f"az account set --subscription {controlPlaneSubId}")

    # grab storage account with control plane cache
    # altan - assume one account for now but technically could be multiple here
    storageAccountsJson = json.loads(execute(f"az storage account list --resource-group {lfoResourceGroupName} --query \"[?starts_with(name,'{CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX}')]\" --output json"))
    accountName = storageAccountsJson["name"]
    controlPlaneId = accountName[len(CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX):]

    # download resources.json to cloud shell local storage
    resourcesCopy = "resourceCache.json"
    execute(f"az storage blob download -f ./{resourcesCopy} --account-name {accountName} -c {CONTROL_PLANE_CONTAINER_NAME} -n {RESOURCES_BLOB_NAME}")
    
    with open(resourcesCopy, "r") as file:
        resourcesJson = json.load(file)

    monitoredSubIds = list(resourcesJson.keys())
    resourceIds = parseResourceIds(resourcesJson)


    # we have rg name, monitored subs and resource IDs now. Run some deletions
    
    deleteRoleAssignments(controlPlaneId)
    for subId in monitoredSubIds:
        deleteResourceGroup(subId, lfoResourceGroupName)
        deleteDiagnosticSettings(subId, resourceIds) #can pass JSON blob into here and parse resources out based on sub ID 
    
     
    
    # resource_group: str | None = "lfo"
    # async with DefaultAzureCredential() as cred, ResourceManagementClient(
    #     cred, sub_id
    # ) as client:
    #     resources = client.resources.list()
    #     jobs, everything_else = partition_resources(
    #         [
    #             resource
    #             async for resource in resources
    #             if should_delete(resource, resource_group)
    #         ]
    #     )

    #     # delete any function apps before we delete the rest to avoid conflicts
    #     for i in range(0, len(jobs), BATCH_SIZE):
    #         await gather(
    #             *[delete_resource(client, r) for r in jobs[i : i + BATCH_SIZE]]
    #         )

    #     for i in range(0, len(everything_else), BATCH_SIZE):
    #         await gather(
    #             *[
    #                 delete_resource(client, r)
    #                 for r in everything_else[i : i + BATCH_SIZE]
    #             ]
    #         )

def parseResourceIds(resourcesJson):
    resourceIds = set() 
    for _, regionDict in resourcesJson.items():
        for resourceId in regionDict.values():
            resourceIds.update(resourceId)

    return resourceIds

if __name__ == "__main__":
    basicConfig(level=INFO)
    DRY_RUN = True #"--dry-run" in argv
    if DRY_RUN:
        log.info("Dry run, no changes will be made")
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