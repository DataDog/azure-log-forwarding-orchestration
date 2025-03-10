#!/usr/bin/env python
"""Script to deploy a personal environment for Azure Log Forwarding Orchestration.

This script handles the deployment of necessary Azure resources for a personal development
environment, including resource groups, storage accounts, container registries, and more.
"""

# stdlib
import argparse
from hashlib import md5
from json import dumps, loads
from logging import INFO, WARNING, basicConfig, getLogger
from os import environ, path
from re import sub
from subprocess import PIPE, Popen
from time import sleep
from typing import Any, Final

# 3p
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
from azure.identity import AzureCliCredential
from azure.mgmt.resource.resources.v2021_01_01 import ResourceManagementClient
from azure.mgmt.resource.resources.v2021_01_01.models import ResourceGroup
from azure.mgmt.storage.v2023_05_01 import StorageManagementClient
from azure.mgmt.storage.v2023_05_01.models import (
    StorageAccountCreateParameters,
    StorageAccountUpdateParameters,
    Sku,
    PublicNetworkAccess,
)
from azure.storage.blob import BlobServiceClient
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Set up logging
basicConfig(level=INFO)
log = getLogger("deploy_personal_env")
getLogger("azure").setLevel(WARNING)

# constants
CONTAINER_NAME: Final = "lfo"
CONTAINER_REGISTRY_MAX_LENGTH: Final = 50
FUNCTION_APP_MAX_LENGTH: Final = 60
MD5_LENGTH: Final = 32
LOCATION: Final = "eastus2"
RESOURCE_GROUP_MAX_LENGTH: Final = 90
STORAGE_ACCOUNT_MAX_LENGTH: Final = 24
MAX_RETRIES: Final = 3
RETRY_WAIT_MULTIPLIER: Final = 1
RETRY_WAIT_MAX: Final = 10
REQUIRED_ENV_VARS: Final = {
    "DD_API_KEY": "Datadog API key",
    "HOME": "User's home directory",
}


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description="Deploy a personal environment for Azure Log Forwarding Orchestration")
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        help="Skip Docker build and push steps",
    )
    parser.add_argument(
        "--force-arm-deploy",
        action="store_true",
        help="Force ARM template deployment even if resources exist",
    )
    parser.add_argument(
        "--location",
        default=LOCATION,
        help=f"Azure region to deploy to (default: {LOCATION})",
    )
    parser.add_argument(
        "--subscription-id",
        help="Azure subscription ID (default: from environment or current account)",
    )
    parser.add_argument(
        "--resource-group-name",
        help="Name of the resource group to create (default: generated from username)",
    )
    parser.add_argument(
        "--storage-account-name",
        help="Name of the storage account to create (default: generated from username)",
    )
    parser.add_argument(
        "--container-registry-name",
        help="Name of the container registry to create (default: generated from username)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually making changes",
    )
    return parser.parse_args()


def validate_environment() -> None:
    """Validate required environment variables and dependencies.

    Raises:
        ValueError: If any required environment variables are missing
        RuntimeError: If required dependencies are not available
    """
    # Check required environment variables
    missing_vars = [var for var, desc in REQUIRED_ENV_VARS.items() if not environ.get(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    # Check for required dependencies
    try:
        run("docker --version")
    except Exception as e:
        raise RuntimeError("Docker is not installed or not accessible") from e

    try:
        run("az --version")
    except Exception as e:
        raise RuntimeError("Azure CLI is not installed or not accessible") from e


def validate_paths(lfo_dir: str) -> None:
    """Validate required paths exist.

    Args:
        lfo_dir: Path to the LFO directory

    Raises:
        ValueError: If required paths do not exist
    """
    required_paths = [
        (lfo_dir, "LFO directory"),
        (path.join(lfo_dir, "ci/deployer-task/Dockerfile"), "Deployer Dockerfile"),
        (path.join(lfo_dir, "ci/scripts/forwarder/build_and_push.sh"), "Forwarder build script"),
        (path.join(lfo_dir, "ci/scripts/control_plane/build_tasks.sh"), "Tasks build script"),
        (path.join(lfo_dir, "control_plane/scripts/publish.py"), "Publish script"),
        (path.join(lfo_dir, "ci/scripts/arm-template/build_initial_run.py"), "ARM template build script"),
        (path.join(lfo_dir, "deploy/azuredeploy.bicep"), "ARM template"),
    ]

    missing_paths = [(p, desc) for p, desc in required_paths if not path.exists(p)]
    if missing_paths:
        raise ValueError(f"Missing required paths: {', '.join(f'{desc} ({p})' for p, desc in missing_paths)}")


def get_name(name: str, max_length: int) -> str:
    """Generate a name that fits within Azure's length constraints.

    Args:
        name: The base name to use
        max_length: Maximum allowed length for the name

    Returns:
        A name that fits within the length constraints, using MD5 hash if needed
    """
    if len(name) > max_length:
        name_bytes = name.encode("utf-8") if isinstance(name, str) else name
        name_md5 = md5(name_bytes).hexdigest()
        if max_length > MD5_LENGTH:
            name = f"{name[: max_length - len(name_md5)]}{name_md5}"
        else:
            name = f"{name[:12]}{name_md5}"[:max_length]
    return name.lower()


def validate_inputs(args: argparse.Namespace) -> None:
    """Validate input parameters.

    Args:
        args: Parsed command line arguments

    Raises:
        ValueError: If any input parameters are invalid
    """
    if args.location and not args.location.isalnum():
        raise ValueError("Location must be alphanumeric")

    if args.subscription_id and not args.subscription_id.replace("-", "").isalnum():
        raise ValueError("Subscription ID must be alphanumeric (with optional hyphens)")

    if args.resource_group_name and len(args.resource_group_name) > RESOURCE_GROUP_MAX_LENGTH:
        raise ValueError(f"Resource group name must be {RESOURCE_GROUP_MAX_LENGTH} characters or less")

    if args.storage_account_name and len(args.storage_account_name) > STORAGE_ACCOUNT_MAX_LENGTH:
        raise ValueError(f"Storage account name must be {STORAGE_ACCOUNT_MAX_LENGTH} characters or less")

    if args.container_registry_name and len(args.container_registry_name) > CONTAINER_REGISTRY_MAX_LENGTH:
        raise ValueError(f"Container registry name must be {CONTAINER_REGISTRY_MAX_LENGTH} characters or less")


@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=RETRY_WAIT_MULTIPLIER, max=RETRY_WAIT_MAX),
    retry=retry_if_exception_type((HttpResponseError, Exception)),
)
def run(cmd: str | list[str], **kwargs: Any) -> str:
    """Run a command and return its stdout, stripping any newlines.

    Args:
        cmd: The command to run, either as a string or list of arguments
        **kwargs: Additional arguments to pass to Popen

    Returns:
        The command's stdout as a string

    Raises:
        Exception: If the command fails
    """
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


def create_resource_group(
    resource_client: ResourceManagementClient,
    resource_group_name: str,
    location: str,
    dry_run: bool = False,
) -> bool:
    """Create a resource group if it doesn't exist.

    Args:
        resource_client: Azure resource management client
        resource_group_name: Name of the resource group to create
        location: Azure region to create the resource group in
        dry_run: If True, only show what would be done

    Returns:
        True if the resource group was created, False if it already existed

    Raises:
        Exception: If resource group creation fails
    """
    try:
        if not resource_client.resource_groups.check_existence(resource_group_name):
            if dry_run:
                log.info("Would create resource group %s in %s", resource_group_name, location)
                return True
            log.info("Resource group %s does not exist, will be created", resource_group_name)
            resource_group = resource_client.resource_groups.create_or_update(
                resource_group_name, ResourceGroup(location=location)
            )
            log.info("Created resource group %s", resource_group.name)
            return True
        return False
    except Exception as e:
        log.error("Failed to create resource group: %s", e)
        raise


def create_storage_account(
    storage_client: StorageManagementClient,
    resource_group_name: str,
    storage_account_name: str,
    location: str,
    dry_run: bool = False,
) -> None:
    """Create a storage account if it doesn't exist.

    Args:
        storage_client: Azure storage management client
        resource_group_name: Name of the resource group
        storage_account_name: Name of the storage account to create
        location: Azure region to create the storage account in
        dry_run: If True, only show what would be done

    Raises:
        Exception: If storage account creation fails
    """
    try:
        storage_client.storage_accounts.get_properties(resource_group_name, storage_account_name)
        log.info("Storage account %s already exists", storage_account_name)
    except ResourceNotFoundError:
        if dry_run:
            log.info(
                "Would create storage account %s in resource group %s in %s",
                storage_account_name,
                resource_group_name,
                location,
            )
            return
        log.info("Storage account %s does not exist, will be created", storage_account_name)
        try:
            poller = storage_client.storage_accounts.begin_create(
                resource_group_name,
                storage_account_name,
                StorageAccountCreateParameters(
                    sku=Sku(name="Standard_LRS"),
                    kind="StorageV2",
                    location=location,
                    public_network_access=PublicNetworkAccess.ENABLED,
                    minimum_tls_version="TLS1_2",
                ),
            )
            account_result = poller.result()
            log.info("Created storage account %s", account_result.name)

            # set the allow_blob_public_access settings
            public_params = StorageAccountUpdateParameters(allow_blob_public_access=True)
            storage_client.storage_accounts.update(resource_group_name, storage_account_name, public_params)
            log.info(
                "Enabled public access for storage account %s. Waiting for settings to take effect...",
                storage_account_name,
            )
            sleep(20)  # wait for storage account setting to propagate
        except Exception as e:
            log.error("Failed to create storage account: %s", e)
            raise


def create_storage_container(
    blob_service_client: BlobServiceClient,
    container_name: str,
    dry_run: bool = False,
) -> None:
    """Create a storage container if it doesn't exist.

    Args:
        blob_service_client: Azure blob service client
        container_name: Name of the container to create
        dry_run: If True, only show what would be done

    Raises:
        SystemExit: If container creation fails
    """
    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        if dry_run:
            log.info("Would create storage container %s", container_name)
            return
        try:
            container_client = blob_service_client.create_container(container_name, public_access="container")
        except Exception as e:
            log.error(
                "Error creating storage container %s. Sometimes this happens due to storage account public permissions not getting applied properly. Please re-try this script.",
                container_name,
            )
            raise SystemExit(1) from e
        log.info("Created storage container %s", container_name)


def create_container_registry(
    resource_group_name: str,
    container_registry_name: str,
    lfo_dir: str,
    dry_run: bool = False,
) -> None:
    """Create a container registry if it doesn't exist.

    Args:
        resource_group_name: Name of the resource group
        container_registry_name: Name of the container registry to create
        lfo_dir: Path to the LFO directory
        dry_run: If True, only show what would be done

    Raises:
        Exception: If container registry creation fails
    """
    acr_list = run(f"az acr list --resource-group {resource_group_name} --output json", cwd=lfo_dir)
    if not acr_list or container_registry_name not in acr_list:
        if dry_run:
            log.info(
                "Would create container registry %s in resource group %s",
                container_registry_name,
                resource_group_name,
            )
            return
        log.info("Attempting to create container registry %s...", container_registry_name)
        run(
            f"az acr create --resource-group {resource_group_name} --name {container_registry_name} --sku Standard",
            cwd=lfo_dir,
        )
        log.info("Created container registry, updating to allow public access...")
        run(
            f"az acr update --name {container_registry_name} --anonymous-pull-enabled",
            cwd=lfo_dir,
        )
        log.info("Waiting for settings to take effect...")
        sleep(20)


@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=RETRY_WAIT_MULTIPLIER, max=RETRY_WAIT_MAX),
    retry=retry_if_exception_type(Exception),
)
def build_and_push_docker_images(
    container_registry_name: str,
    lfo_dir: str,
    dry_run: bool = False,
) -> None:
    """Build and push Docker images to the container registry.

    Args:
        container_registry_name: Name of the container registry
        lfo_dir: Path to the LFO directory
        dry_run: If True, only show what would be done

    Raises:
        Exception: If Docker operations fail
    """
    if dry_run:
        log.info("Would build and push Docker images to %s.azurecr.io", container_registry_name)
        return

    # login to ACR
    login_output = run(
        f"az acr login --name {container_registry_name}",
        cwd=lfo_dir,
    )
    log.info(login_output)

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


def deploy_lfo(
    resource_group_name: str,
    location: str,
    subscription_id: str,
    container_registry_name: str,
    storage_account_name: str,
    lfo_dir: str,
    dry_run: bool = False,
) -> None:
    """Deploy LFO using ARM template.

    Args:
        resource_group_name: Name of the resource group
        location: Azure region to deploy to
        subscription_id: Azure subscription ID
        container_registry_name: Name of the container registry
        storage_account_name: Name of the storage account
        lfo_dir: Path to the LFO directory
        dry_run: If True, only show what would be done

    Raises:
        Exception: If deployment fails
    """
    if dry_run:
        log.info(
            "Would deploy LFO to resource group %s in %s using container registry %s and storage account %s",
            resource_group_name,
            location,
            container_registry_name,
            storage_account_name,
        )
        return

    log.info("Building ARM template...")
    run("./ci/scripts/arm-template/build_initial_run.py", cwd=lfo_dir)
    log.info(f"Deploying LFO to {resource_group_name}...")
    api_key = environ["DD_API_KEY"]
    params = {
        "monitoredSubscriptions": dumps([subscription_id]),
        "controlPlaneLocation": location,
        "controlPlaneSubscriptionId": subscription_id,
        "controlPlaneResourceGroupName": resource_group_name,
        "datadogApiKey": api_key,
        "datadogTelemetry": "true",
        "piiScrubberRules": environ.get("PII_SCRUBBER_RULES", ""),
        "datadogSite": environ.get("DD_SITE", "datadoghq.com"),
        "imageRegistry": f"{container_registry_name}.azurecr.io",
        "storageAccountUrl": f"https://{storage_account_name}.blob.core.windows.net",
        "logLevel": "DEBUG",
    }
    run(
        [
            *("az", "deployment", "mg", "create"),
            *("--management-group-id", "Azure-Integrations-Mg"),
            *("--location", location),
            *("--name", resource_group_name),
            *("--template-file", "./deploy/azuredeploy.bicep"),
            *(paramPart for k, v in params.items() for paramPart in ("--parameters", f"{k}={v}")),
        ],
        cwd=lfo_dir,
    )


def start_deployer_job(resource_group_name: str, lfo_dir: str, dry_run: bool = False) -> None:
    """Start the deployer job.

    Args:
        resource_group_name: Name of the resource group
        lfo_dir: Path to the LFO directory
        dry_run: If True, only show what would be done

    Raises:
        SystemExit: If deployer job is not found
    """
    jobs = loads(run(f"az containerapp job list --resource-group {resource_group_name} --output json"))
    if not jobs or not (
        deployer_job := next(
            (job.get("name") for job in jobs if "deployer-task" in job.get("name")),
            None,
        )
    ):
        log.error(
            "Deployer not found, try re-running the script with `--force-arm-deploy` to ensure all resources are created"
        )
        raise SystemExit(1)

    if dry_run:
        log.info("Would start deployer job %s in resource group %s", deployer_job, resource_group_name)
        return

    run(f"az containerapp job start --resource-group {resource_group_name} --name {deployer_job}")
    log.info(f"Deployer job {deployer_job} executed! In a minute or two, all tasks will be redeployed.")


def main() -> None:
    """Main entry point for the script."""
    args = parse_args()
    validate_inputs(args)
    validate_environment()

    # shared variables
    home = environ.get("HOME")
    user = environ.get("USER")
    lfo_base_name = sub(r"\W+", "", environ.get("LFO_BASE_NAME", f"lfo{user}"))
    lfo_dir = f"{home}/dd/azure-log-forwarding-orchestration"
    subscription_id = (
        args.subscription_id or environ.get("AZURE_SUBSCRIPTION_ID") or run("az account show --query id -o tsv")
    )

    try:
        validate_paths(lfo_dir)

        credential = AzureCliCredential()
        resource_client = ResourceManagementClient(credential, subscription_id)
        storage_client = StorageManagementClient(credential, subscription_id)

        # set az cli to look at the correct subscription
        run(f"az account set --subscription {subscription_id}")

        # generate names
        resource_group_name = args.resource_group_name or get_name(lfo_base_name, RESOURCE_GROUP_MAX_LENGTH)
        storage_account_name = args.storage_account_name or get_name(lfo_base_name, STORAGE_ACCOUNT_MAX_LENGTH)
        container_registry_name = args.container_registry_name or get_name(lfo_base_name, CONTAINER_REGISTRY_MAX_LENGTH)

        if args.dry_run:
            log.info("DRY RUN: The following changes would be made:")
            log.info("- Resource Group: %s", resource_group_name)
            log.info("- Storage Account: %s", storage_account_name)
            log.info("- Container Registry: %s", container_registry_name)
            log.info("- Location: %s", args.location)
            log.info("- Subscription ID: %s", subscription_id)
            log.info("")

        # create resource group
        initial_deploy = create_resource_group(resource_client, resource_group_name, args.location, args.dry_run)

        # create storage account
        create_storage_account(storage_client, resource_group_name, storage_account_name, args.location, args.dry_run)

        if not args.dry_run:
            # get connection string and create container
            keys = storage_client.storage_accounts.list_keys(resource_group_name, storage_account_name)
            if not keys or not keys.keys:
                raise Exception("Failed to get storage account keys")
            connection_string = f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName={storage_account_name};AccountKey={keys.keys[0].value}"
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            create_storage_container(blob_service_client, CONTAINER_NAME, args.dry_run)

            # create container registry
            create_container_registry(resource_group_name, container_registry_name, lfo_dir, args.dry_run)

            # build and push Docker images if needed
            if not args.skip_docker:
                build_and_push_docker_images(container_registry_name, lfo_dir, args.dry_run)

            # build current version of tasks
            run(f"{lfo_dir}/ci/scripts/control_plane/build_tasks.sh", cwd=lfo_dir)

            # upload current version of tasks to storage account
            run(
                f"{lfo_dir}/control_plane/scripts/publish.py https://{storage_account_name}.blob.core.windows.net {connection_string}",
                cwd=lfo_dir,
            )

            # deploy LFO if needed
            if initial_deploy or args.force_arm_deploy:
                deploy_lfo(
                    resource_group_name,
                    args.location,
                    subscription_id,
                    container_registry_name,
                    storage_account_name,
                    lfo_dir,
                    args.dry_run,
                )
            else:
                start_deployer_job(resource_group_name, lfo_dir, args.dry_run)

        if args.dry_run:
            log.info("DRY RUN: No changes were made")

    except Exception as e:
        log.error("Deployment failed: %s", e)
        raise SystemExit(1) from e


if __name__ == "__main__":
    main()
