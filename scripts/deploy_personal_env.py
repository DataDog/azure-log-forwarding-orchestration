#!/usr/bin/env python
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

from argparse import ArgumentParser
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import md5
from json import dumps, loads
from os import environ
from re import sub
from subprocess import PIPE, Popen
from time import sleep
from typing import Any
from urllib.parse import quote

from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import (
    AccountSasPermissions,
    BlobServiceClient,
    ResourceTypes,
    generate_account_sas,
)

MD5_LENGTH = 32

CONTAINER_REGISTRY_MAX_LENGTH = 50
LOCATION = "eastus2"
RESOURCE_GROUP_MAX_LENGTH = 90
STORAGE_ACCOUNT_MAX_LENGTH = 24

# RBAC role definition IDs
CONTRIBUTOR_ROLE = "b24988ac-6180-42a0-ab88-20f7382dd24c"
MONITORING_READER_ROLE = "43d0d8ad-25c7-4714-9337-8ba259a9fe05"
MONITORING_CONTRIBUTOR_ROLE = "749f88d5-cbae-40b8-bcfc-e573ddc772fa"
READER_AND_DATA_ACCESS_ROLE = "c12c1c16-33a1-487b-954d-41c89c60f349"
STORAGE_BLOB_DATA_READER_ROLE = "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1"

# FunctionApp-specific constants
FA_CONTAINER_NAME = "lfo"

# ContainerAppJob-specific constants
# Matches BLOB_STORAGE_CACHE in cache/common.py; must be created explicitly because there is no
# Azure Functions runtime in the CAJ environment to create it automatically.
CAJ_CACHE_CONTAINER_NAME = "control-plane-cache"
TASK_SCHEDULE = "*/5 * * * *"
TASK_CPU = "0.5"
TASK_MEMORY = "1Gi"
TASK_REPLICA_TIMEOUTS = {
    "resources-task": "300",
    "diagnostic-settings-task": "300",
    "scaling-task": "600",
}
TASK_NAMES = ["resources-task", "scaling-task", "diagnostic-settings-task"]


@dataclass
class AzureContext:
    home: str
    user: str
    lfo_base_name: str
    lfo_dir: str
    subscription_id: str
    credential: Any
    resource_client: Any
    storage_client: Any
    resource_group_name: str
    storage_account_name: str
    container_registry_name: str


def get_name(name: str, max_length: int) -> str:
    if len(name) > max_length:
        name_md5 = md5(name.encode()).hexdigest()
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


def setup_azure_context(base_name_suffix: str = "") -> AzureContext:
    home = environ["HOME"]
    user = environ["USER"]
    lfo_base_name = sub(
        r"\W+", "", environ.get("LFO_BASE_NAME", f"lfo{user}{base_name_suffix}")
    )
    lfo_dir = f"{home}/dd/azure-log-forwarding-orchestration"

    subscription_id = environ.get("AZURE_SUBSCRIPTION_ID") or run(
        "az account show --query id -o tsv"
    )
    run(f"az account set --subscription {subscription_id}")

    credential = AzureCliCredential()
    resource_client = ResourceManagementClient(credential, subscription_id)
    storage_client = StorageManagementClient(credential, subscription_id)

    return AzureContext(
        home=home,
        user=user,
        lfo_base_name=lfo_base_name,
        lfo_dir=lfo_dir,
        subscription_id=subscription_id,
        credential=credential,
        resource_client=resource_client,
        storage_client=storage_client,
        resource_group_name=get_name(lfo_base_name, RESOURCE_GROUP_MAX_LENGTH),
        storage_account_name=get_name(lfo_base_name, STORAGE_ACCOUNT_MAX_LENGTH),
        container_registry_name=get_name(lfo_base_name, CONTAINER_REGISTRY_MAX_LENGTH),
    )


def ensure_resource_group(ctx: AzureContext) -> bool:
    """Creates the resource group if it doesn't exist. Returns True if it was just created."""
    if ctx.resource_client.resource_groups.check_existence(ctx.resource_group_name):
        return False
    print(f"Resource group {ctx.resource_group_name} does not exist, will be created")
    resource_group = ctx.resource_client.resource_groups.create_or_update(
        ctx.resource_group_name, {"location": LOCATION}
    )
    print(f"Created resource group {resource_group.name}")
    return True


def ensure_storage_account(ctx: AzureContext) -> None:
    availability_result = ctx.storage_client.storage_accounts.check_name_availability(
        {"name": ctx.storage_account_name}
    )
    if not availability_result.name_available:
        return
    print(f"Attempting to create storage account {ctx.storage_account_name}...")
    poller = ctx.storage_client.storage_accounts.begin_create(
        ctx.resource_group_name,
        ctx.storage_account_name,
        {
            "location": LOCATION,
            "kind": "StorageV2",
            "sku": {"name": "Standard_LRS"},
            "properties": {
                "supportsHttpsTrafficOnly": True,
                "allowBlobPublicAccess": False,
            },
        },
    )
    account_result = poller.result()
    print(f"Created storage account {account_result.name}")


def get_storage_connection(ctx: AzureContext):
    """Returns (keys, connection_string, blob_service_client)."""
    keys = ctx.storage_client.storage_accounts.list_keys(
        ctx.resource_group_name, ctx.storage_account_name
    )
    connection_string = (
        f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;"
        f"AccountName={ctx.storage_account_name};AccountKey={keys.keys[0].value}"
    )
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    return keys, connection_string, blob_service_client


def ensure_blob_container(
    blob_service_client: BlobServiceClient, container_name: str
) -> None:
    container_client = blob_service_client.get_container_client(container_name)
    if container_client.exists():
        return
    try:
        blob_service_client.create_container(container_name)
    except Exception as e:
        print(f"Error creating storage container {container_name}.")
        raise SystemExit(1) from e
    print(f"Created storage container {container_name}")


def ensure_acr(ctx: AzureContext) -> None:
    acr_list = run(
        f"az acr list --resource-group {ctx.resource_group_name} --output json",
        cwd=ctx.lfo_dir,
    )
    if acr_list and ctx.container_registry_name in acr_list:
        return
    print(f"Attempting to create container registry {ctx.container_registry_name}...")
    run(
        f"az acr create --resource-group {ctx.resource_group_name} --name {ctx.container_registry_name} --sku Standard",
        cwd=ctx.lfo_dir,
    )
    print("Created container registry, updating to allow public access...")
    run(
        f"az acr update --name {ctx.container_registry_name} --anonymous-pull-enabled",
        cwd=ctx.lfo_dir,
    )
    print("Waiting for settings to take effect...")
    sleep(20)


def grant_role(principal_id: str, role_id: str, scope: str) -> None:
    """Grant a role assignment, skipping silently if it already exists."""
    try:
        run(
            [
                "az", "role", "assignment", "create",
                "--assignee-object-id", principal_id,
                "--assignee-principal-type", "ServicePrincipal",
                "--role", role_id,
                "--scope", scope,
            ],
            stderr=PIPE,
        )
        print(f"Granted role {role_id} to {scope}")
    except Exception as e:
        if "RoleAssignmentExists" in str(e):
            print(f"Role {role_id} on {scope} already assigned, skipping")
        else:
            raise


def acr_login(ctx: AzureContext) -> None:
    login_output = run(
        f"az acr login --name {ctx.container_registry_name}", cwd=ctx.lfo_dir
    )
    print(login_output)


def build_and_push_forwarder(ctx: AzureContext, commit_sha: str) -> None:
    run(
        f"ci/scripts/forwarder/build_and_push.sh {ctx.container_registry_name}.azurecr.io latest {commit_sha}",
        cwd=ctx.lfo_dir,
    )


def parse_args():
    parser = ArgumentParser(description="Deploy a personal LFO environment")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--function-apps", action="store_true", help="Deploy a FunctionApp env")
    mode.add_argument("--container-app-jobs", action="store_true", help="Deploy a ContainerAppJob env")
    parser.add_argument("--skip-docker", action="store_true", help="Skip docker build/push")
    parser.add_argument(
        "--force-arm-deploy",
        action="store_true",
        help="(FunctionApp mode) Force ARM/Bicep deployment even if the resource group already exists",
    )
    parser.add_argument(
        "--force-recreate",
        action="store_true",
        help="(ContainerAppJob mode) Force recreate Container App Jobs even if they already exist",
    )
    return parser.parse_args()


def deploy_function_apps(
    ctx: AzureContext, commit_sha: str, skip_docker: bool, force_arm_deploy: bool
) -> None:
    integrations_management_dir = f"{ctx.home}/dd/integrations-management"

    initial_deploy = ensure_resource_group(ctx)
    ensure_storage_account(ctx)
    keys, connection_string, blob_service_client = get_storage_connection(ctx)
    ensure_blob_container(blob_service_client, FA_CONTAINER_NAME)
    ensure_acr(ctx)

    if not skip_docker:
        acr_login(ctx)
        run(
            f"docker buildx build --platform=linux/amd64 --build-arg VERSION_TAG={commit_sha} "
            f"--tag {ctx.container_registry_name}.azurecr.io/deployer:latest "
            f"-f {ctx.lfo_dir}/ci/deployer-task/Dockerfile ./control_plane --push",
            cwd=ctx.lfo_dir,
        )
        build_and_push_forwarder(ctx, commit_sha)

    run(
        f"{ctx.lfo_dir}/ci/scripts/control_plane/set_version.sh {commit_sha}",
        cwd=ctx.lfo_dir,
    )
    run(f"{ctx.lfo_dir}/ci/scripts/control_plane/build_tasks.sh", cwd=ctx.lfo_dir)
    run(
        f"{ctx.lfo_dir}/ci/scripts/control_plane/unset_version.sh {commit_sha}",
        cwd=ctx.lfo_dir,
    )
    run(
        f"{ctx.lfo_dir}/control_plane/scripts/publish.py"
        f" https://{ctx.storage_account_name}.blob.core.windows.net {connection_string}",
        cwd=ctx.lfo_dir,
    )

    if initial_deploy or force_arm_deploy:
        _arm_deploy(ctx, keys, integrations_management_dir)
    else:
        _run_deployer(ctx)


def _arm_deploy(ctx: AzureContext, keys, integrations_management_dir: str) -> None:
    print(
        f"Deploying LFO to {ctx.resource_group_name}...\n"
        "\tCheck progress in the portal: "
        f"https://portal.azure.com/#view/HubsExtension/DeploymentDetailsBlade/~/overview/id/"
        f"%2Fproviders%2FMicrosoft.Management%2FmanagementGroups%2FAzure-Integrations-Mg"
        f"%2Fproviders%2FMicrosoft.Resources%2Fdeployments%2F{quote(ctx.resource_group_name, safe='')}"
    )
    api_key = environ["DD_API_KEY"]
    params = {
        "monitoredSubscriptions": dumps([ctx.subscription_id]),
        "controlPlaneLocation": LOCATION,
        "controlPlaneSubscriptionId": ctx.subscription_id,
        "controlPlaneResourceGroupName": ctx.resource_group_name,
        "datadogApiKey": api_key,
        "datadogTelemetry": "true",
        "piiScrubberRules": environ.get("PII_SCRUBBER_RULES", ""),
        "resourceTagFilters": environ.get("RESOURCE_TAG_FILTERS", ""),
        "datadogSite": environ.get("DD_SITE", "datadoghq.com"),
        "imageRegistry": f"{ctx.container_registry_name}.azurecr.io",
        "storageAccountUrl": f"https://{ctx.storage_account_name}.blob.core.windows.net",
        "storageAccountSas": generate_account_sas(
            account_name=ctx.storage_account_name,
            account_key=keys.keys[0].value,
            resource_types=ResourceTypes(object=True),
            permission=AccountSasPermissions(read=True),
            expiry=datetime.now(UTC) + timedelta(hours=1),
        ),
        "logLevel": "DEBUG",
    }
    run(
        [
            *("az", "deployment", "mg", "create"),
            *("--management-group-id", "Azure-Integrations-Mg"),
            *("--location", LOCATION),
            *("--name", ctx.resource_group_name),
            *(
                "--template-file",
                f"{integrations_management_dir}/azure/logging_install/bicep/azuredeploy.bicep",
            ),
            *(
                param_part
                for k, v in params.items()
                for param_part in ("--parameters", f"{k}={v}")
            ),
        ],
        cwd=ctx.lfo_dir,
    )

    deployer_jobs = loads(
        run(
            f"az containerapp job list --resource-group {ctx.resource_group_name} --output json"
        )
    )
    deployer_job_name = next(
        (
            job.get("name")
            for job in deployer_jobs
            if "deployer-task" in job.get("name")
        ),
        None,
    )
    if deployer_job_name:
        deployer_principal_id = loads(
            run(
                f"az containerapp job show --resource-group {ctx.resource_group_name}"
                f" --name {deployer_job_name} --query identity.principalId --output json"
            )
        )
        storage_account_id = (
            f"/subscriptions/{ctx.subscription_id}/resourceGroups/{ctx.resource_group_name}"
            f"/providers/Microsoft.Storage/storageAccounts/{ctx.storage_account_name}"
        )
        STORAGE_BLOB_DATA_READER = "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1"
        run(
            f"az role assignment create --assignee-object-id {deployer_principal_id}"
            f" --assignee-principal-type ServicePrincipal"
            f" --role {STORAGE_BLOB_DATA_READER} --scope {storage_account_id}",
        )
        print(
            f"Granted Storage Blob Data Reader to deployer {deployer_job_name}"
            f" on {ctx.storage_account_name}"
        )


def _run_deployer(ctx: AzureContext) -> None:
    jobs = loads(
        run(
            f"az containerapp job list --resource-group {ctx.resource_group_name} --output json"
        )
    )
    if not jobs or not (
        deployer_job := next(
            (job.get("name") for job in jobs if "deployer-task" in job.get("name")),
            None,
        )
    ):
        print(
            "Deployer not found, try re-running the script with `--force-arm-deploy`"
            " to ensure all resources are created"
        )
        raise SystemExit(1)
    run(
        f"az containerapp job start --resource-group {ctx.resource_group_name} --name {deployer_job}"
    )
    print(
        f"Deployer job {deployer_job} executed! In a minute or two, all tasks will be redeployed."
    )


def deploy_container_app_jobs(
    ctx: AzureContext, commit_sha: str, skip_docker: bool, force_recreate: bool
) -> None:
    control_plane_env_name = f"control-plane-env-{ctx.lfo_base_name}"
    deployer_env_name = f"dd-log-forwarder-env-{ctx.lfo_base_name}-{LOCATION}"

    ensure_resource_group(ctx)
    ensure_storage_account(ctx)
    _, connection_string, blob_service_client = get_storage_connection(ctx)
    ensure_blob_container(blob_service_client, CAJ_CACHE_CONTAINER_NAME)
    ensure_blob_container(blob_service_client, FA_CONTAINER_NAME)
    ensure_acr(ctx)

    if not skip_docker:
        acr_login(ctx)
        build_and_push_forwarder(ctx, commit_sha)
        for task in TASK_NAMES:
            print(f"Building and pushing {task}:{commit_sha}...")
            run(
                [
                    "docker",
                    "buildx",
                    "build",
                    "--platform",
                    "linux/amd64",
                    "--build-arg",
                    f"VERSION_TAG={commit_sha}",
                    "--tag",
                    f"{ctx.container_registry_name}.azurecr.io/{task}:{commit_sha}",
                    "--tag",
                    f"{ctx.container_registry_name}.azurecr.io/{task}:latest",
                    "-f",
                    f"{ctx.lfo_dir}/ci/{task}/Dockerfile",
                    "./control_plane",
                    "--push",
                ],
                cwd=ctx.lfo_dir,
            )
        print("Building and pushing deployer-caj:latest...")
        run(
            [
                "docker",
                "buildx",
                "build",
                "--platform",
                "linux/amd64",
                "--build-arg",
                f"VERSION_TAG={commit_sha}",
                "--tag",
                f"{ctx.container_registry_name}.azurecr.io/deployer-caj:latest",
                "-f",
                f"{ctx.lfo_dir}/ci/deployer-caj-task/Dockerfile",
                "./control_plane",
                "--push",
            ],
            cwd=ctx.lfo_dir,
        )
        run(
            f"{ctx.lfo_dir}/control_plane/scripts/publish_images.py"
            f" https://{ctx.storage_account_name}.blob.core.windows.net"
            f" {ctx.container_registry_name}.azurecr.io"
            f" {commit_sha}"
            f" {connection_string}",
            cwd=ctx.lfo_dir,
        )

    _ensure_control_plane_env(ctx, control_plane_env_name)
    _ensure_control_plane_env(ctx, deployer_env_name)
    _deploy_tasks(
        ctx, control_plane_env_name, commit_sha, connection_string, force_recreate
    )
    _deploy_deployer_caj(ctx, deployer_env_name, connection_string, force_recreate)

    print(
        f"\nDone! Control plane ContainerAppJob environment deployed to resource group '{ctx.resource_group_name}'.\n"
        f"Tasks are scheduled with cron '{TASK_SCHEDULE}' in environment '{control_plane_env_name}'."
    )


def _deploy_deployer_caj(
    ctx: AzureContext,
    control_plane_env_name: str,
    connection_string: str,
    force_recreate: bool,
) -> None:
    deployer_job_name = f"deployer-task-{ctx.lfo_base_name}"
    deployer_caj_image = f"{ctx.container_registry_name}.azurecr.io/deployer-caj:latest"
    storage_account_url = f"https://{ctx.storage_account_name}.blob.core.windows.net"
    api_key = environ["DD_API_KEY"]
    dd_site = environ.get("DD_SITE", "datadoghq.com")

    env_vars = [
        f"SUBSCRIPTION_ID={ctx.subscription_id}",
        f"RESOURCE_GROUP={ctx.resource_group_name}",
        f"CONTROL_PLANE_REGION={LOCATION}",
        f"CONTROL_PLANE_ID={ctx.lfo_base_name}",
        f"STORAGE_ACCOUNT_URL={storage_account_url}",
        f"DD_SITE={dd_site}",
        "DD_API_KEY=secretref:dd-api-key",
        "AzureWebJobsStorage=secretref:connection-string",
    ]

    existing_jobs = loads(
        run(f"az containerapp job list --resource-group {ctx.resource_group_name} --output json")
    )
    existing_job_names = {job["name"] for job in existing_jobs}

    if deployer_job_name not in existing_job_names or force_recreate:
        print(f"Creating Container App Job {deployer_job_name}...")
        run(
            [
                "az", "containerapp", "job", "create",
                "--resource-group", ctx.resource_group_name,
                "--name", deployer_job_name,
                "--environment", control_plane_env_name,
                "--trigger-type", "Schedule",
                "--cron-expression", "*/30 * * * *",
                "--image", deployer_caj_image,
                "--cpu", TASK_CPU,
                "--memory", TASK_MEMORY,
                "--replica-timeout", "1800",
                "--replica-retry-limit", "0",
                "--replica-completion-count", "1",
                "--parallelism", "1",
                "--mi-system-assigned",
                "--secrets",
                f"dd-api-key={api_key}",
                f"connection-string={connection_string}",
                "--env-vars",
                *env_vars,
            ],
        )
        print(f"Created Container App Job {deployer_job_name}")
    else:
        print(f"Updating Container App Job {deployer_job_name} image to {deployer_caj_image}...")
        run(
            [
                "az", "containerapp", "job", "update",
                "--resource-group", ctx.resource_group_name,
                "--name", deployer_job_name,
                "--image", deployer_caj_image,
            ],
        )
        print(f"Updated Container App Job {deployer_job_name}")

    principal_id = loads(
        run(
            f"az containerapp job show --resource-group {ctx.resource_group_name}"
            f" --name {deployer_job_name} --query identity.principalId --output json"
        )
    )
    resource_group_scope = (
        f"/subscriptions/{ctx.subscription_id}/resourceGroups/{ctx.resource_group_name}"
    )
    storage_account_id = (
        f"/subscriptions/{ctx.subscription_id}/resourceGroups/{ctx.resource_group_name}"
        f"/providers/Microsoft.Storage/storageAccounts/{ctx.storage_account_name}"
    )
    grant_role(principal_id, CONTRIBUTOR_ROLE, resource_group_scope)
    grant_role(principal_id, STORAGE_BLOB_DATA_READER_ROLE, storage_account_id)


def _ensure_control_plane_env(ctx: AzureContext, control_plane_env_name: str) -> None:
    existing_envs = run(
        f"az containerapp env list --resource-group {ctx.resource_group_name} --output json",
        cwd=ctx.lfo_dir,
    )
    if existing_envs and control_plane_env_name in existing_envs:
        return
    print(f"Creating Container Apps managed environment {control_plane_env_name}...")
    run(
        [
            "az",
            "containerapp",
            "env",
            "create",
            "--resource-group",
            ctx.resource_group_name,
            "--name",
            control_plane_env_name,
            "--location",
            LOCATION,
        ],
        cwd=ctx.lfo_dir,
    )
    print(f"Created Container Apps managed environment {control_plane_env_name}")


def _deploy_tasks(
    ctx: AzureContext,
    control_plane_env_name: str,
    commit_sha: str,
    connection_string: str,
    force_recreate: bool,
) -> None:
    api_key = environ["DD_API_KEY"]
    dd_site = environ.get("DD_SITE", "datadoghq.com")
    forwarder_image = f"{ctx.container_registry_name}.azurecr.io/forwarder:latest"

    common_env_vars = [
        f"CONTROL_PLANE_REGION={LOCATION}",
        f"CONTROL_PLANE_ID={ctx.lfo_base_name}",
        f"RESOURCE_GROUP={ctx.resource_group_name}",
        f"DD_SITE={dd_site}",
        f"MONITORED_SUBSCRIPTIONS={dumps([ctx.subscription_id])}",
        "LOG_LEVEL=DEBUG",
        "DD_TELEMETRY=true",
        "DD_API_KEY=secretref:dd-api-key",
        "AzureWebJobsStorage=secretref:connection-string",
    ]

    task_extra_env_vars: dict[str, list[str]] = {
        "resources-task": [
            f"RESOURCE_TAG_FILTERS={environ.get('RESOURCE_TAG_FILTERS', '')}"
        ],
        "scaling-task": [
            f"FORWARDER_IMAGE={forwarder_image}",
            f"PII_SCRUBBER_RULES={environ.get('PII_SCRUBBER_RULES', '')}",
        ],
        "diagnostic-settings-task": [],
    }

    subscription_scope = f"/subscriptions/{ctx.subscription_id}"
    resource_group_scope = (
        f"/subscriptions/{ctx.subscription_id}/resourceGroups/{ctx.resource_group_name}"
    )
    task_role_assignments: dict[str, list[tuple[str, str]]] = {
        "resources-task": [(MONITORING_READER_ROLE, subscription_scope)],
        "scaling-task": [(CONTRIBUTOR_ROLE, resource_group_scope)],
        "diagnostic-settings-task": [
            (READER_AND_DATA_ACCESS_ROLE, resource_group_scope),
            (MONITORING_CONTRIBUTOR_ROLE, subscription_scope),
        ],
    }

    existing_jobs = loads(
        run(
            f"az containerapp job list --resource-group {ctx.resource_group_name} --output json"
        )
    )
    existing_job_names = {job["name"] for job in existing_jobs}

    for task in TASK_NAMES:
        job_name = f"{task}-{ctx.lfo_base_name}"
        task_image = f"{ctx.container_registry_name}.azurecr.io/{task}:{commit_sha}"
        env_vars = common_env_vars + task_extra_env_vars[task]

        if job_name not in existing_job_names or force_recreate:
            print(f"Creating Container App Job {job_name}...")
            run(
                [
                    "az",
                    "containerapp",
                    "job",
                    "create",
                    "--resource-group",
                    ctx.resource_group_name,
                    "--name",
                    job_name,
                    "--environment",
                    control_plane_env_name,
                    "--trigger-type",
                    "Schedule",
                    "--cron-expression",
                    TASK_SCHEDULE,
                    "--image",
                    task_image,
                    "--cpu",
                    TASK_CPU,
                    "--memory",
                    TASK_MEMORY,
                    "--replica-timeout",
                    TASK_REPLICA_TIMEOUTS[task],
                    "--replica-retry-limit",
                    "0",
                    "--replica-completion-count",
                    "1",
                    "--parallelism",
                    "1",
                    "--mi-system-assigned",
                    "--secrets",
                    f"dd-api-key={api_key}",
                    f"connection-string={connection_string}",
                    "--env-vars",
                    *env_vars,
                ],
            )
            print(f"Created Container App Job {job_name}")
        else:
            print(f"Updating Container App Job {job_name} image to {task_image}...")
            run(
                [
                    "az",
                    "containerapp",
                    "job",
                    "update",
                    "--resource-group",
                    ctx.resource_group_name,
                    "--name",
                    job_name,
                    "--image",
                    task_image,
                ],
            )
            print(f"Updated Container App Job {job_name}")

        principal_id = loads(
            run(
                f"az containerapp job show --resource-group {ctx.resource_group_name}"
                f" --name {job_name} --query identity.principalId --output json"
            )
        )
        for role_id, scope in task_role_assignments[task]:
            grant_role(principal_id, role_id, scope)


def main():
    args = parse_args()
    base_name_suffix = "caj" if args.container_app_jobs else ""
    ctx = setup_azure_context(base_name_suffix)
    commit_sha = run("git rev-parse --short HEAD", cwd=ctx.lfo_dir)

    if args.container_app_jobs:
        deploy_container_app_jobs(ctx, commit_sha, args.skip_docker, args.force_recreate)
    else:
        deploy_function_apps(ctx, commit_sha, args.skip_docker, args.force_arm_deploy)


if __name__ == "__main__":
    main()
