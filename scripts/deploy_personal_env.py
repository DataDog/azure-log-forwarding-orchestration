#!/usr/bin/env python
# stdlib
from hashlib import md5
from json import dumps, loads
from os import environ
from re import sub
from subprocess import PIPE, Popen
from sys import argv
from time import sleep
from typing import Any

# azure
from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.storage.v2019_06_01.models import StorageAccountUpdateParameters
from azure.storage.blob import BlobServiceClient

# constants
CONTAINER_NAME = "lfo"
CONTAINER_REGISTRY_MAX_LENGTH = 50
FUNCTION_APP_MAX_LENGTH = 60
MD5_LENGTH = 32
LOCATION = "eastus2"
RESOURCE_GROUP_MAX_LENGTH = 90
STORAGE_ACCOUNT_MAX_LENGTH = 24

# options
SKIP_DOCKER = "--skip-docker" in argv
FORCE_ARM_DEPLOY = "--force-arm-deploy" in argv


# functions
def get_name(name: str, max_length: int) -> str:
    if len(name) > max_length:
        name_bytes = name
        if not isinstance(name, (bytes | bytearray)):
            name_bytes = name.encode("utf-8")
        name_md5 = md5(name_bytes).hexdigest()
        if max_length > MD5_LENGTH:
            name = f"{name[: max_length - len(name_md5)]}{name_md5}"
        else:
            name = f"{name[:12]}{name_md5}"[:max_length]
    return name.lower()


def run(cmd: str | list[str], **kwargs: Any) -> str:
    """Runs the command and returns the stdout, stripping any newlines"""
    if isinstance(cmd, str):
        cmd = cmd.split()
    output = Popen(cmd, stdout=PIPE, text=True, **kwargs)
    output.wait()
    if not output.stdout:
        return ""
    if output.returncode != 0:
        err = "" if not output.stderr else output.stderr.read()
        raise Exception(f"Error running command {cmd}: {err}")
    return output.stdout.read().strip()


# shared variables
home = environ.get("HOME")
user = environ.get("USER")
lfo_base_name = sub(r"\W+", "", environ.get("LFO_BASE_NAME", f"lfo{user}"))
lfo_dir = f"{home}/dd/azure-log-forwarding-orchestration"
subscription_id = environ.get("AZURE_SUBSCRIPTION_ID") or run("az account show --query id -o tsv")
credential = AzureCliCredential()
resource_client = ResourceManagementClient(credential, subscription_id)
storage_client = StorageManagementClient(credential, subscription_id)
initial_deploy = False


# set az cli to look at the correct subscription
run(f"az account set --subscription {subscription_id}")


# generate name
resource_group_name = get_name(lfo_base_name, RESOURCE_GROUP_MAX_LENGTH)


# if resource group does not exist, create it
if not resource_client.resource_groups.check_existence(resource_group_name):
    print(f"Resource group {resource_group_name} does not exist, will be created")
    resource_group = resource_client.resource_groups.create_or_update(resource_group_name, {"location": LOCATION})
    initial_deploy = True
    print(f"Created resource group {resource_group.name}")


# check if staging storage account exists
storage_account_name = get_name(lfo_base_name, STORAGE_ACCOUNT_MAX_LENGTH)
availability_result = storage_client.storage_accounts.check_name_availability({"name": storage_account_name})


# if storage account does not exist, create it
if availability_result.name_available:
    print(f"Attempting to create storage account {storage_account_name}...")
    poller = storage_client.storage_accounts.begin_create(
        resource_group_name,
        storage_account_name,
        {"location": LOCATION, "kind": "StorageV2", "sku": {"name": "Standard_LRS"}},
    )

    account_result = poller.result()
    print(f"Created storage account {account_result.name}")

    # set the allow_blob_public_access settings
    public_params = StorageAccountUpdateParameters(allow_blob_public_access=True)
    storage_client.storage_accounts.update(resource_group_name, storage_account_name, public_params)
    print(f"Enabled public access for storage account {storage_account_name}. Waiting for settings to take effect...")
    sleep(20)  # wait for storage account setting to propagate


# get connection string
keys = storage_client.storage_accounts.list_keys(resource_group_name, storage_account_name)
connection_string = f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName={storage_account_name};AccountKey={keys.keys[0].value}"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)


container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# if container does not exist, create it
if not container_client.exists():
    try:
        container_client = blob_service_client.create_container(CONTAINER_NAME, public_access="container")
    except Exception as e:
        print(
            f"Error creating storage container {CONTAINER_NAME}. Sometimes this happens due to storage account public permissions not getting applied properly. Please re-try this script."
        )
        raise SystemExit(1) from e
    print(f"Created storage container {CONTAINER_NAME}")


# check if ACR exists
container_registry_name = get_name(lfo_base_name, CONTAINER_REGISTRY_MAX_LENGTH)
acr_list = run(f"az acr list --resource-group {resource_group_name} --output json", cwd=lfo_dir)

# if ACR does not exist, create it
if not acr_list or container_registry_name not in acr_list:
    print(f"Attempting to create container registry {container_registry_name}...")
    run(
        f"az acr create --resource-group {resource_group_name} --name {container_registry_name} --sku Standard",
        cwd=lfo_dir,
    )
    print("Created container registry, updating to allow public access...")
    run(
        f"az acr update --name {container_registry_name} --anonymous-pull-enabled",
        cwd=lfo_dir,
    )
    print("Waiting for settings to take effect...")
    sleep(20)


if not SKIP_DOCKER:
    # login to ACR
    login_output = run(
        f"az acr login --name {container_registry_name}",
        cwd=lfo_dir,
    )
    print(login_output)

    # build and push deployer
    run(
        f"docker buildx build --platform=linux/amd64 --tag {container_registry_name}.azurecr.io/deployer:latest "
        + f"-f {lfo_dir}/ci/deployer-task/Dockerfile ./control_plane --push",
        cwd=lfo_dir,
    )

    # build and push forwarder
    run(
        f"ci/scripts/forwarder/build_and_push.sh {container_registry_name}.azurecr.io latest",
        cwd=lfo_dir,
    )


# build current version of tasks
run(f"{lfo_dir}/ci/scripts/control_plane/build_tasks.sh", cwd=lfo_dir)

# upload current version of tasks to storage account
run(
    f"{lfo_dir}/control_plane/scripts/publish.py https://{storage_account_name}.blob.core.windows.net {connection_string}",
    cwd=lfo_dir,
)

# deployment has not happened, deploy LFO
if initial_deploy or FORCE_ARM_DEPLOY:
    print("Building ARM template...")
    run("./ci/scripts/arm-template/build_initial_run.py", cwd=lfo_dir)
    print(f"Deploying LFO to {resource_group_name}...")
    api_key = environ["DD_API_KEY"]
    params = {
        "monitoredSubscriptions": dumps([subscription_id]),
        "controlPlaneLocation": LOCATION,
        "controlPlaneSubscriptionId": subscription_id,
        "controlPlaneResourceGroupName": resource_group_name,
        "datadogApiKey": api_key,
        "datadogTelemetry": "true",
        "datadogSite": environ.get("DD_SITE", "datadoghq.com"),
        "imageRegistry": f"{container_registry_name}.azurecr.io",
        "storageAccountUrl": f"https://{storage_account_name}.blob.core.windows.net",
        "resourceTagFilter": environ.get("RESOURCE_TAG_FILTER", ""),
        "logLevel": "DEBUG",
    }
    run(
        [
            *("az", "deployment", "mg", "create"),
            *("--management-group-id", "Azure-Integrations-Mg"),
            *("--location", LOCATION),
            *("--name", resource_group_name),
            *("--template-file", "./deploy/azuredeploy.bicep"),
            *(paramPart for k, v in params.items() for paramPart in ("--parameters", f"{k}={v}")),
        ],
        cwd=lfo_dir,
    )
else:
    # execute deployer
    jobs = loads(run(f"az containerapp job list --resource-group {resource_group_name} --output json"))
    if not jobs or not (
        deployer_job := next(
            (job.get("name") for job in jobs if "deployer-task" in job.get("name")),
            None,
        )
    ):
        print(
            "Deployer not found, try re-running the script with `--force-arm-deploy` to ensure all resources are created"
        )
        raise SystemExit(1)

    run(f"az containerapp job start --resource-group {resource_group_name} --name {deployer_job}")
    print(f"Deployer job {deployer_job} executed! In a minute or two, all tasks will be redeployed.")
