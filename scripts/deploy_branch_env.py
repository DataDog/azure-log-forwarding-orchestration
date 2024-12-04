#!/usr/bin/env python
# stdlib
from hashlib import md5
from json import loads
from os import environ
from re import sub
from subprocess import Popen, PIPE
from sys import exit
from time import sleep

# azure
from azure.core.exceptions import ResourceNotFoundError
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


# functions
def get_name(name: str, max_length: int) -> str:
    if len(name) > max_length:
        name_bytes = name
        if not isinstance(name, (bytes, bytearray)):
            name_bytes = name.encode("utf-8")
        name_md5 = md5(name_bytes).hexdigest()
        if max_length > MD5_LENGTH:
            name = f"{name[:max_length - len(name_md5)]}{name_md5}"
        else:
            name = f"{name[:12]}{name_md5}"[:max_length]
    return name.lower()


# shared variables
home = environ.get("HOME")
user = environ.get("USER")
lfo_dir = f"{home}/dd/azure-log-forwarding-orchestration"
subscription_id = environ.get("AZURE_SUBSCRIPTION_ID")
credential = AzureCliCredential()
resource_client = ResourceManagementClient(credential, subscription_id)
storage_client = StorageManagementClient(credential, subscription_id)


# set az cli to look at the correct subscription
set_subscription_output = Popen(
    ["az", "account", "set", "--subscription", subscription_id], stdout=PIPE
)
set_subscription_output.wait()


# generate name from branch
raw_branch_output = Popen(
    ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=lfo_dir, stdout=PIPE
)
branch = raw_branch_output.stdout.readline().decode("utf-8").strip()
if user.lower() in branch.lower():
    branch = branch.replace(user, "")
branch = sub(r"\W+", "", branch)

resource_group_name = get_name(branch, RESOURCE_GROUP_MAX_LENGTH)


# check if resource group exists for branch
resource_group = None
try:
    resource_group = resource_client.resource_groups.get(resource_group_name)
except ResourceNotFoundError:
    print(f"Resource group {resource_group_name} does not exist, will be created")


# if resource group does not exist, create it
if not resource_group:
    resource_group = resource_client.resource_groups.create_or_update(
        resource_group_name, {"location": LOCATION}
    )
    print(f"Created resource group {resource_group.name}")


# check if staging storage account exists for branch
storage_account_name = get_name(branch, STORAGE_ACCOUNT_MAX_LENGTH)
availability_result = storage_client.storage_accounts.check_name_availability(
    {"name": storage_account_name}
)


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
    storage_client.storage_accounts.update(
        resource_group_name, storage_account_name, public_params
    )
    print(
        f"Enabled public access for storage account {storage_account_name}. Waiting for settings to take effect..."
    )
    sleep(20)  # wait for storage account setting to propagate


# get connection string
keys = storage_client.storage_accounts.list_keys(
    resource_group_name, storage_account_name
)
connection_string = f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName={storage_account_name};AccountKey={keys.keys[0].value}"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)


container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# if container does not exist, create it
if not container_client.exists():
    try:
        container_client = blob_service_client.create_container(
            CONTAINER_NAME, public_access="container"
        )
    except Exception:
        print(
            f"Error creating storage container {CONTAINER_NAME}. Sometimes this happens due to storage account public permissions not getting applied properly. Please re-try this script."
        )
        exit(1)
    print(f"Created storage container {CONTAINER_NAME}")


# check if ACR exists for branch
container_registry_name = get_name(branch, CONTAINER_REGISTRY_MAX_LENGTH)
list_acr_output = Popen(
    ["az", "acr", "list", "--resource-group", resource_group_name, "--output", "json"],
    cwd=lfo_dir,
    stdout=PIPE,
)
list_acr_output.wait()
acr_list = list_acr_output.stdout.read().decode("utf-8")

# if ACR does not exist, create it
if not acr_list or container_registry_name not in acr_list:
    print(f"Attempting to create container registry {container_registry_name}...")
    acr_create_ouput = Popen(
        [
            "az",
            "acr",
            "create",
            "--resource-group",
            resource_group_name,
            "--name",
            container_registry_name,
            "--sku",
            "Standard",
        ],
        cwd=lfo_dir,
        stdout=PIPE,
    )
    acr_create_ouput.wait()
    print("Created container registry, updating to allow public access...")
    acr_update_output = Popen(
        [
            "az",
            "acr",
            "update",
            "--name",
            container_registry_name,
            "--anonymous-pull-enabled",
        ],
        cwd=lfo_dir,
        stdout=PIPE,
    )
    print("Waiting for settings to take effect...")
    sleep(20)


# login to ACR
login_output = Popen(
    ["az", "acr", "login", "--name", container_registry_name], cwd=lfo_dir, stdout=PIPE
)
login_output.wait()
print(login_output.stdout.readline().decode("utf-8"))


# build and push deployer
deployer_build_output = Popen(
    [
        "docker",
        "buildx",
        "build",
        "--platform=linux/amd64",
        "--tag",
        f"{container_registry_name}.azurecr.io/deployer:latest",
        "-f",
        f"{lfo_dir}/ci/deployer-task/Dockerfile",
        "./control_plane",
        "--push",
    ],
    cwd=lfo_dir,
    stdout=PIPE,
)
deployer_build_output.wait()


# build and push forwarder
forwarder_build_output = Popen(
    [
        "ci/scripts/forwarder/build_and_push.sh",
        f"{container_registry_name}.azurecr.io",
        "latest",
    ],
    cwd=lfo_dir,
    stdout=PIPE,
)
forwarder_build_output.wait()


# build current version of tasks
Popen([f"{lfo_dir}/ci/scripts/control_plane/build_tasks.sh"], cwd=lfo_dir).wait()

# upload current version of tasks to storage account
raw_publish_output = Popen(
    [
        f"{lfo_dir}/ci/scripts/control_plane/publish.py",
        f"https://{storage_account_name}.blob.core.windows.net",
        connection_string,
    ],
    cwd=lfo_dir,
    stdout=PIPE,
)
raw_publish_output.wait()
print(raw_publish_output.stdout.readline().decode("utf-8"))


lfo_resource_group_name = get_name(f"{branch}lfo", RESOURCE_GROUP_MAX_LENGTH)

# check if a deployment has already happened
lfo_resource_group = None
try:
    lfo_resource_group = resource_client.resource_groups.get(lfo_resource_group_name)
except ResourceNotFoundError:
    print(
        f"Resource group {lfo_resource_group_name} does not exist, will proceed with LFO deployment"
    )


# deployment has not happened, deploy LFO
if not lfo_resource_group:
    print(f"Deploying LFO to {lfo_resource_group_name}...")
    app_key = environ.get("DD_APP_KEY")
    api_key = environ.get("DD_API_KEY")
    deploy_output = Popen(
        [
            "az",
            "deployment",
            "mg",
            "create",
            "--management-group-id",
            "Azure-Integrations-Mg",
            "--location",
            LOCATION,
            "--name",
            lfo_resource_group_name,
            "--template-file",
            "./deploy/azuredeploy.bicep",
            "--parameters",
            f'monitoredSubscriptions=["{subscription_id}"]',
            "--parameters",
            f"controlPlaneLocation={LOCATION}",
            "--parameters",
            f"controlPlaneSubscriptionId={subscription_id}",
            "--parameters",
            f"controlPlaneResourceGroupName={lfo_resource_group_name}",
            "--parameters",
            f"datadogApplicationKey={app_key}",
            "--parameters",
            f"datadogApiKey={api_key}",
            "--parameters",
            "datadogSite=datadoghq.com",
            "--parameters",
            f"imageRegistry={container_registry_name}.azurecr.io",
            "--parameters",
            f"storageAccountUrl=https://{storage_account_name}.blob.core.windows.net",
        ],
        cwd=lfo_dir,
        stdout=PIPE,
    )
    deploy_output.wait()


# execute deployer
job_found = False
while not job_found:
    list_jobs_output = Popen(
        [
            "az",
            "containerapp",
            "job",
            "list",
            "--resource-group",
            lfo_resource_group_name,
            "--output",
            "json",
        ],
        stdout=PIPE,
    )
    list_jobs_output.wait()
    jobs_json = list_jobs_output.stdout.read().decode("utf-8")
    if not jobs_json:
        sleep(5)
        continue
    jobs = loads(jobs_json)
    print("execute deployer")
    for job in jobs:
        if "deployer-task" in job.get("name"):
            job_found = True
            print(f"Executing deployer for job {job.get('name')}...")
            deployer_output = Popen(
                [
                    "az",
                    "containerapp",
                    "job",
                    "start",
                    "--resource-group",
                    lfo_resource_group_name,
                    "--name",
                    job.get("name"),
                ],
                stdout=PIPE,
            )
            deployer_output.wait()
            print(f"Deployer executed for job {job.get('name')}")
            break
