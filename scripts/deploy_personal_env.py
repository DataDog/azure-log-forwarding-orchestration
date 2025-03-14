#!/usr/bin/env python
"""
Azure Log Forwarding Orchestration (LFO) Personal Environment Deployment Script

This script automates the deployment of a personal LFO environment in Azure.
It creates or updates the necessary resources including resource groups,
storage accounts, container registries, and deploys the LFO components.

Usage:
    ./deploy_personal_env.py [--skip-docker] [--force-arm-deploy] [--config CONFIG_FILE] [--generate-config OUTPUT_FILE]

Options:
    --skip-docker       Skip Docker image building and pushing
    --force-arm-deploy  Force ARM template deployment even if resources exist
    --config CONFIG_FILE Path to the configuration file (optional, default: ./scripts/config.yaml)
    --generate-config OUTPUT_FILE Generate a sample configuration file at the specified path and exit
"""

# stdlib
import os
import sys
import time
import json
import hashlib
import logging
import re
import subprocess
import argparse
from typing import Any, Dict, List, Tuple, Union
from urllib.parse import quote

# Try to import YAML
try:
    import yaml
except ImportError:
    print("Required dependency 'pyyaml' not found. Please install it with:")
    print("pip install pyyaml")
    sys.exit(1)

# Example configuration file template
# This serves as documentation for users to understand available configuration options
# Copy this to a file and modify as needed
CONFIG_TEMPLATE = """# Azure Log Forwarding Orchestration (LFO) Deployment Configuration
# Copy this template to a file (e.g., config.yaml) and modify as needed
# Then run the script with: ./deploy_personal_env.py --config config.yaml

# Azure configuration
azure:
  # Azure region for resource deployment
  location: "eastus2"
  # Azure subscription ID (optional, will use default if not specified)
  subscription_id: ""
  # Base name for resources (will use "lfo{username}" if not specified)
  base_name: ""

# Datadog configuration
datadog:
  # Datadog site (default: datadoghq.com)
  site: "datadoghq.com"
  # API key (can also be set via DD_API_KEY environment variable)
  api_key: ""
  # Enable Datadog telemetry (true/false)
  telemetry_enabled: true
  # PII scrubber rules (can also be set via PII_SCRUBBER_RULES environment variable)
  pii_scrubber_rules: ""

# Deployment options
deployment:
  # Log level for deployed services (DEBUG, INFO, WARN, ERROR)
  log_level: "DEBUG"
  # List of subscription IDs to monitor (empty array means current subscription only)
  monitored_subscriptions: []
  # Path to LFO directory (default: ~/dd/azure-log-forwarding-orchestration)
  lfo_dir: ""

# Docker options
docker:
  # Skip Docker image building and pushing (true/false)
  skip_docker: false
  # Tags for Docker images
  deployer_tag: "latest"
  forwarder_tag: "latest"

# Logging configuration for the deployment script
logging:
  # Log level (DEBUG, INFO, WARN, ERROR)
  level: "INFO"
  # Include timestamps in logs (true/false)
  include_timestamps: true
"""

# Constants
CONTAINER_NAME = "lfo"
CONTAINER_REGISTRY_MAX_LENGTH = 50
FUNCTION_APP_MAX_LENGTH = 60
MD5_LENGTH = 32
LOCATION = "eastus2"
RESOURCE_GROUP_MAX_LENGTH = 90
STORAGE_ACCOUNT_MAX_LENGTH = 24

# Required paths for validation
REQUIRED_PATHS = [
    "ci/deployer-task/Dockerfile",
    "ci/scripts/forwarder/build_and_push.sh",
    "ci/scripts/control_plane/build_tasks.sh",
    "control_plane/scripts/publish.py",
    "deploy/azuredeploy.bicep",
]

# Required environment variables
REQUIRED_ENV_VARS = ["DD_API_KEY"]


# Parse command line arguments
def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Deploy Azure Log Forwarding Orchestration")
    parser.add_argument("--skip-docker", action="store_true", help="Skip Docker image building and pushing")
    parser.add_argument(
        "--force-arm-deploy", action="store_true", help="Force ARM template deployment even if resources exist"
    )
    parser.add_argument("--config", default="./scripts/config.yaml", help="Path to the configuration file (optional)")
    parser.add_argument(
        "--generate-config", metavar="OUTPUT_FILE", help="Generate a sample configuration file and exit"
    )
    return parser.parse_args()


# Load configuration
def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from a YAML file.

    Args:
        config_path: Path to the configuration file.

    Returns:
        A dictionary containing the configuration.
    """
    if not config_path:
        print("No configuration file specified, using default values.")
        print("You can create a configuration file using the template at the top of this script.")
        return {}

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        return config if config else {}
    except FileNotFoundError:
        print(f"Configuration file '{config_path}' not found, using default values.")
        print("You can create a configuration file using the following template:")
        print("\nTo generate a sample configuration file, run:")
        print(f"  python {sys.argv[0]} --generate-config [output_file]")
        return {}
    except yaml.YAMLError as e:
        print(f"Error parsing configuration file: {e}")
        print("Using default values instead.")
        return {}
    except Exception as e:
        print(f"Unexpected error reading configuration file: {e}")
        print("Using default values instead.")
        return {}


# Set up logging
def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    """
    Set up logging based on configuration.

    Args:
        config: The configuration dictionary.

    Returns:
        A configured logger.
    """
    # Ensure config is not None
    if config is None:
        config = {}

    log_config = config.get("logging", {})

    # Get log level with fallback to INFO
    log_level_name = log_config.get("level", "INFO")
    try:
        log_level = getattr(logging, log_level_name)
    except AttributeError:
        print(f"Invalid log level '{log_level_name}', using INFO")
        log_level = logging.INFO

    if log_config.get("include_timestamps", True):
        logging.basicConfig(
            level=log_level, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
    else:
        logging.basicConfig(level=log_level, format="%(levelname)s - %(message)s")

    logger = logging.getLogger("lfo-deploy")

    # Log configuration status
    if not config:
        logger.info("No configuration file loaded, using default values")

    return logger


class ProgressIndicator:
    """Simple progress indicator for long-running operations."""

    def __init__(self, message: str):
        self.message = message
        self.running = False
        self.start_time = 0.0

    def start(self) -> None:
        """Start the progress indicator."""
        self.running = True
        self.start_time = time.time()
        logger.info(f"{self.message}...")

    def stop(self, success: bool = True) -> None:
        """Stop the progress indicator and show completion message."""
        self.running = False
        elapsed = time.time() - self.start_time
        status = "completed" if success else "failed"
        logger.info(f"{self.message} {status} in {elapsed:.2f} seconds")


def validate_environment(config: Dict[str, Any]) -> None:
    """
    Validate that all required environment variables are set and dependencies are installed.

    Args:
        config: The configuration dictionary.

    Raises:
        SystemExit: If any required environment variable is missing or dependency is not installed.
    """
    # Ensure config is not None
    if config is None:
        config = {}

    # Check required environment variables
    required_vars = REQUIRED_ENV_VARS.copy()

    # If Datadog API key is in config, we don't need it in environment
    if config.get("datadog", {}).get("api_key"):
        if "DD_API_KEY" in required_vars:
            required_vars.remove("DD_API_KEY")

    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    # Check if az CLI is installed
    try:
        run("az --version", capture_output=False)
    except Exception:
        logger.error("Azure CLI (az) is not installed or not in PATH. Please install it.")
        sys.exit(1)

    # Check if Docker is installed if we're not skipping Docker
    skip_docker = config.get("docker", {}).get("skip_docker", False) or args.skip_docker
    if not skip_docker:
        try:
            run("docker --version", capture_output=False)
        except Exception:
            logger.error("Docker is not installed or not in PATH. Please install it or use --skip-docker.")
            sys.exit(1)

    logger.info("Environment validation successful")


def validate_paths(base_dir: str) -> None:
    """
    Validate that all required files and directories exist.

    Args:
        base_dir: The base directory of the LFO project.

    Raises:
        SystemExit: If any required file or directory is missing.
    """
    missing_paths = []
    for path in REQUIRED_PATHS:
        full_path = os.path.join(base_dir, path)
        if not os.path.exists(full_path):
            missing_paths.append(path)

    if missing_paths:
        logger.error(f"Missing required files or directories: {', '.join(missing_paths)}")
        logger.error("Please ensure you're running this script from the correct directory.")
        sys.exit(1)

    logger.info("Path validation successful")


def get_name(name: str, max_length: int) -> str:
    """
    Generate a name that fits within the specified maximum length.
    If the name is too long, it will be truncated and an MD5 hash will be appended.

    Args:
        name: The original name.
        max_length: The maximum allowed length for the name.

    Returns:
        A name that fits within the specified maximum length.
    """
    if len(name) > max_length:
        name_bytes = name.encode("utf-8") if isinstance(name, str) else name
        name_md5 = hashlib.md5(name_bytes).hexdigest()
        if max_length > MD5_LENGTH:
            name = f"{name[: max_length - len(name_md5)]}{name_md5}"
        else:
            name = f"{name[:12]}{name_md5}"[:max_length]
    return name.lower()


def run(cmd: Union[str, List[str]], capture_output: bool = True, **kwargs: Any) -> str:
    """
    Run a command and return its output.

    Args:
        cmd: The command to run, either as a string or list of strings.
        capture_output: Whether to capture and return the command output.
        **kwargs: Additional keyword arguments to pass to subprocess.Popen.

    Returns:
        The command output as a string, with trailing newlines stripped.

    Raises:
        Exception: If the command returns a non-zero exit code.
    """
    if isinstance(cmd, str):
        cmd_list = cmd.split()
        cmd_str = cmd
    else:
        cmd_list = cmd
        cmd_str = " ".join(cmd)

    logger.debug(f"Running command: {cmd_str}")

    if capture_output:
        process = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, **kwargs)
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            error_msg = f"Error running command {cmd_str}: {stderr}"
            logger.error(error_msg)
            raise Exception(error_msg)

        return stdout.strip()
    else:
        process = subprocess.run(cmd_list, check=True, **kwargs)
        return ""


def create_or_update_resource_group(resource_client: Any, resource_group_name: str, location: str) -> Tuple[Any, bool]:
    """
    Create or update an Azure resource group.

    Args:
        resource_client: The Azure Resource Management client.
        resource_group_name: The name of the resource group.
        location: The Azure region where the resource group should be created.

    Returns:
        A tuple containing the resource group object and a boolean indicating if it was newly created.
    """
    progress = ProgressIndicator(f"Checking resource group {resource_group_name}")
    progress.start()

    try:
        # Check if resource group exists
        exists = resource_client.resource_groups.check_existence(resource_group_name)

        if not exists:
            logger.info(f"Resource group {resource_group_name} does not exist, creating it")
            resource_group = resource_client.resource_groups.create_or_update(
                resource_group_name, {"location": location}
            )
            progress.stop(True)
            logger.info(f"Created resource group {resource_group.name}")
            return resource_group, True
        else:
            # Resource group exists, get its details
            resource_group = resource_client.resource_groups.get(resource_group_name)
            progress.stop(True)
            logger.info(f"Using existing resource group {resource_group.name}")
            return resource_group, False
    except Exception as e:
        progress.stop(False)
        logger.error(f"Error creating/updating resource group: {str(e)}")
        raise


def create_or_update_storage_account(
    storage_client: Any, resource_group_name: str, storage_account_name: str, location: str
) -> Tuple[Any, str]:
    """
    Create or update an Azure storage account.

    Args:
        storage_client: The Azure Storage Management client.
        resource_group_name: The name of the resource group.
        storage_account_name: The name of the storage account.
        location: The Azure region where the storage account should be created.

    Returns:
        A tuple containing the storage account object and its connection string.
    """
    progress = ProgressIndicator(f"Checking storage account {storage_account_name}")
    progress.start()

    try:
        # Check if storage account exists
        availability_result = storage_client.storage_accounts.check_name_availability({"name": storage_account_name})

        if availability_result.name_available:
            logger.info(f"Storage account {storage_account_name} does not exist, creating it")

            # Create storage account
            poller = storage_client.storage_accounts.begin_create(
                resource_group_name,
                storage_account_name,
                {"location": location, "kind": "StorageV2", "sku": {"name": "Standard_LRS"}},
            )

            account_result = poller.result()
            logger.info(f"Created storage account {account_result.name}")

            # Enable blob public access
            from azure.mgmt.storage.v2019_06_01.models import StorageAccountUpdateParameters

            public_params = StorageAccountUpdateParameters(allow_blob_public_access=True)
            storage_client.storage_accounts.update(resource_group_name, storage_account_name, public_params)

            logger.info(f"Enabled public access for storage account {storage_account_name}")
            logger.info("Waiting for settings to take effect...")
            time.sleep(20)  # wait for storage account setting to propagate
        else:
            logger.info(f"Using existing storage account {storage_account_name}")
            account_result = storage_client.storage_accounts.get(resource_group_name, storage_account_name)

        # Get connection string
        keys = storage_client.storage_accounts.list_keys(resource_group_name, storage_account_name)
        connection_string = (
            f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;"
            f"AccountName={storage_account_name};AccountKey={keys.keys[0].value}"
        )

        progress.stop(True)
        return account_result, connection_string
    except Exception as e:
        progress.stop(False)
        logger.error(f"Error creating/updating storage account: {str(e)}")
        raise


def create_or_update_storage_container(blob_service_client: Any, container_name: str) -> None:
    """
    Create or update a storage container.

    Args:
        blob_service_client: The Azure Blob Service client.
        container_name: The name of the container.
    """
    progress = ProgressIndicator(f"Checking storage container {container_name}")
    progress.start()

    try:
        container_client = blob_service_client.get_container_client(container_name)

        if not container_client.exists():
            try:
                container_client = blob_service_client.create_container(container_name, public_access="container")
                logger.info(f"Created storage container {container_name}")
            except Exception as e:
                progress.stop(False)
                logger.error(
                    f"Error creating storage container {container_name}. "
                    "Sometimes this happens due to storage account public permissions "
                    "not getting applied properly. Please re-try this script."
                )
                raise SystemExit(1) from e
        else:
            logger.info(f"Using existing storage container {container_name}")

        progress.stop(True)
    except Exception as e:
        progress.stop(False)
        logger.error(f"Error creating/updating storage container: {str(e)}")
        raise


def create_or_update_container_registry(
    resource_group_name: str, container_registry_name: str, location: str, lfo_dir: str, config: Dict[str, Any]
) -> None:
    """
    Create or update an Azure Container Registry.

    Args:
        resource_group_name: The name of the resource group.
        container_registry_name: The name of the container registry.
        location: The Azure region where the container registry should be created.
        lfo_dir: The base directory of the LFO project.
        config: The configuration dictionary.
    """
    # Ensure config is not None
    if config is None:
        config = {}

    progress = ProgressIndicator(f"Checking container registry {container_registry_name}")
    progress.start()

    try:
        # Check if ACR exists
        acr_list_json = run(f"az acr list --resource-group {resource_group_name} --output json", cwd=lfo_dir)
        acr_list = json.loads(acr_list_json) if acr_list_json else []
        acr_exists = any(acr["name"] == container_registry_name for acr in acr_list)

        if not acr_exists:
            logger.info(f"Container registry {container_registry_name} does not exist, creating it")

            # Create ACR
            run(
                f"az acr create --resource-group {resource_group_name} "
                f"--name {container_registry_name} --sku Standard",
                cwd=lfo_dir,
            )

            logger.info("Created container registry, updating to allow public access...")

            # Enable anonymous pull
            run(f"az acr update --name {container_registry_name} --anonymous-pull-enabled", cwd=lfo_dir)

            logger.info("Waiting for settings to take effect...")
            time.sleep(20)
        else:
            logger.info(f"Using existing container registry {container_registry_name}")

        progress.stop(True)
    except Exception as e:
        progress.stop(False)
        logger.error(f"Error creating/updating container registry: {str(e)}")
        raise


def build_and_push_docker_images(container_registry_name: str, lfo_dir: str, config: Dict[str, Any]) -> None:
    """
    Build and push Docker images to the container registry.

    Args:
        container_registry_name: The name of the container registry.
        lfo_dir: The base directory of the LFO project.
        config: The configuration dictionary.
    """
    # Ensure config is not None
    if config is None:
        config = {}

    skip_docker = config.get("docker", {}).get("skip_docker", False) or args.skip_docker
    if skip_docker:
        logger.info("Skipping Docker image building and pushing")
        return

    try:
        # Login to ACR
        progress = ProgressIndicator(f"Logging in to container registry {container_registry_name}")
        progress.start()
        login_output = run(f"az acr login --name {container_registry_name}", cwd=lfo_dir)
        progress.stop(True)
        logger.info(login_output)

        # Get Docker tags from config
        deployer_tag = config.get("docker", {}).get("deployer_tag", "latest")
        forwarder_tag = config.get("docker", {}).get("forwarder_tag", "latest")

        # Build and push deployer
        progress = ProgressIndicator("Building and pushing deployer image")
        progress.start()
        run(
            f"docker buildx build --platform=linux/amd64 "
            f"--tag {container_registry_name}.azurecr.io/deployer:{deployer_tag} "
            f"-f {lfo_dir}/ci/deployer-task/Dockerfile ./control_plane --push",
            cwd=lfo_dir,
        )
        progress.stop(True)

        # Build and push forwarder
        progress = ProgressIndicator("Building and pushing forwarder image")
        progress.start()
        run(f"ci/scripts/forwarder/build_and_push.sh {container_registry_name}.azurecr.io {forwarder_tag}", cwd=lfo_dir)
        progress.stop(True)
    except Exception as e:
        logger.error(f"Error building and pushing Docker images: {str(e)}")
        raise


def build_and_upload_tasks(lfo_dir: str, storage_account_name: str, connection_string: str) -> None:
    """
    Build and upload tasks to the storage account.

    Args:
        lfo_dir: The base directory of the LFO project.
        storage_account_name: The name of the storage account.
        connection_string: The storage account connection string.
    """
    try:
        # Build tasks
        progress = ProgressIndicator("Building tasks")
        progress.start()
        run(f"{lfo_dir}/ci/scripts/control_plane/build_tasks.sh", cwd=lfo_dir)
        progress.stop(True)

        # Upload tasks
        progress = ProgressIndicator("Uploading tasks to storage account")
        progress.start()
        run(
            f"{lfo_dir}/control_plane/scripts/publish.py "
            f"https://{storage_account_name}.blob.core.windows.net {connection_string}",
            cwd=lfo_dir,
        )
        progress.stop(True)
    except Exception as e:
        logger.error(f"Error building and uploading tasks: {str(e)}")
        raise


def deploy_lfo(
    resource_group_name: str,
    subscription_id: str,
    location: str,
    container_registry_name: str,
    storage_account_name: str,
    lfo_dir: str,
    config: Dict[str, Any],
) -> None:
    """
    Deploy the LFO using ARM templates.

    Args:
        resource_group_name: The name of the resource group.
        subscription_id: The Azure subscription ID.
        location: The Azure region where the resources should be deployed.
        container_registry_name: The name of the container registry.
        storage_account_name: The name of the storage account.
        lfo_dir: The base directory of the LFO project.
        config: The configuration dictionary.
    """
    # Ensure config is not None
    if config is None:
        config = {}

    logger.info(
        f"Deploying LFO to {resource_group_name}...\n"
        f"\tCheck progress in the portal: "
        f"https://portal.azure.com/#view/HubsExtension/DeploymentDetailsBlade/~/overview/id/"
        f"%2Fproviders%2FMicrosoft.Management%2FmanagementGroups%2FAzure-Integrations-Mg%2F"
        f"providers%2FMicrosoft.Resources%2Fdeployments%2F{quote(resource_group_name, safe='')}"
    )

    # Get parameters from config or environment variables
    datadog_config = config.get("datadog", {})
    deployment_config = config.get("deployment", {})

    api_key = datadog_config.get("api_key") or os.environ.get("DD_API_KEY", "")
    pii_scrubber_rules = datadog_config.get("pii_scrubber_rules") or os.environ.get("PII_SCRUBBER_RULES", "")
    datadog_site = datadog_config.get("site", "datadoghq.com")
    telemetry_enabled = str(datadog_config.get("telemetry_enabled", True)).lower()
    log_level = deployment_config.get("log_level", "DEBUG")

    # Get monitored subscriptions
    monitored_subscriptions = deployment_config.get("monitored_subscriptions", [])
    if not monitored_subscriptions:
        monitored_subscriptions = [subscription_id]

    # Prepare deployment parameters
    params = {
        "monitoredSubscriptions": json.dumps(monitored_subscriptions),
        "controlPlaneLocation": location,
        "controlPlaneSubscriptionId": subscription_id,
        "controlPlaneResourceGroupName": resource_group_name,
        "datadogApiKey": api_key,
        "datadogTelemetry": telemetry_enabled,
        "piiScrubberRules": pii_scrubber_rules,
        "datadogSite": datadog_site,
        "imageRegistry": f"{container_registry_name}.azurecr.io",
        "storageAccountUrl": f"https://{storage_account_name}.blob.core.windows.net",
        "logLevel": log_level,
    }

    # Build command for ARM deployment
    cmd = [
        "az",
        "deployment",
        "mg",
        "create",
        "--management-group-id",
        "Azure-Integrations-Mg",
        "--location",
        location,
        "--name",
        resource_group_name,
        "--template-file",
        "./deploy/azuredeploy.bicep",
    ]

    # Add parameters to command
    for k, v in params.items():
        cmd.extend(["--parameters", f"{k}={v}"])

    # Execute deployment
    progress = ProgressIndicator("Deploying LFO resources")
    progress.start()
    try:
        run(cmd, cwd=lfo_dir)
        progress.stop(True)
        logger.info("LFO deployment completed successfully")
    except Exception as e:
        progress.stop(False)
        logger.error(f"Error deploying LFO: {str(e)}")
        raise


def execute_deployer_job(resource_group_name: str, lfo_dir: str) -> None:
    """
    Execute the deployer job to redeploy all tasks.

    Args:
        resource_group_name: The name of the resource group.
        lfo_dir: The base directory of the LFO project.
    """
    progress = ProgressIndicator("Finding deployer job")
    progress.start()

    try:
        # Get list of container app jobs
        jobs_json = run(f"az containerapp job list --resource-group {resource_group_name} --output json", cwd=lfo_dir)
        jobs = json.loads(jobs_json) if jobs_json else []

        # Find deployer job
        deployer_job = next((job.get("name") for job in jobs if "deployer-task" in job.get("name", "")), None)

        if not deployer_job:
            progress.stop(False)
            logger.error(
                "Deployer not found, try re-running the script with `--force-arm-deploy` "
                "to ensure all resources are created"
            )
            raise SystemExit(1)

        progress.stop(True)

        # Execute deployer job
        progress = ProgressIndicator(f"Executing deployer job {deployer_job}")
        progress.start()
        run(f"az containerapp job start --resource-group {resource_group_name} --name {deployer_job}", cwd=lfo_dir)
        progress.stop(True)

        logger.info(f"Deployer job {deployer_job} executed! " "In a minute or two, all tasks will be redeployed.")
    except Exception as e:
        progress.stop(False)
        logger.error(f"Error executing deployer job: {str(e)}")
        raise


def generate_config_file(output_path: str) -> None:
    """
    Generate a sample configuration file.

    Args:
        output_path: Path where the configuration file should be written.
    """
    try:
        with open(output_path, "w") as f:
            f.write(CONFIG_TEMPLATE)
        print(f"Sample configuration file generated at: {output_path}")
        print("Edit this file with your desired settings and run the script with:")
        print(f"  python {sys.argv[0]} --config {output_path}")
        sys.exit(0)
    except Exception as e:
        print(f"Error generating configuration file: {e}")
        sys.exit(1)


def main() -> None:
    """Main function to deploy the LFO environment."""
    try:
        # Load configuration
        config = load_config(args.config)

        # If no config was loaded, inform the user about the --generate-config option
        if not config:
            logger.info("Tip: You can generate a sample configuration file with:")
            logger.info(f"  python {sys.argv[0]} --generate-config your_config.yaml")

        # Get shared variables
        home = os.environ.get("HOME", "")
        user = os.environ.get("USER", "")

        # Get values from config or defaults
        azure_config = config.get("azure", {})
        deployment_config = config.get("deployment", {})

        lfo_base_name = azure_config.get("base_name") or os.environ.get("LFO_BASE_NAME", f"lfo{user}")
        lfo_base_name = re.sub(r"\W+", "", lfo_base_name)

        lfo_dir = deployment_config.get("lfo_dir") or os.environ.get(
            "LFO_DIR", f"{home}/dd/azure-log-forwarding-orchestration"
        )
        location = azure_config.get("location", LOCATION)

        # Log key configuration values
        logger.info(f"Using base name: {lfo_base_name}")
        logger.info(f"Using LFO directory: {lfo_dir}")
        logger.info(f"Using Azure location: {location}")

        # Validate environment and paths
        validate_environment(config)
        validate_paths(lfo_dir)

        # Get Azure subscription ID
        subscription_id = (
            azure_config.get("subscription_id")
            or os.environ.get("AZURE_SUBSCRIPTION_ID")
            or run("az account show --query id -o tsv")
        )
        if not subscription_id:
            logger.error("Could not determine Azure subscription ID")
            sys.exit(1)

        logger.info(f"Using Azure subscription ID: {subscription_id}")

        # Initialize Azure clients
        try:
            from azure.identity import AzureCliCredential
            from azure.mgmt.resource import ResourceManagementClient
            from azure.mgmt.storage import StorageManagementClient
            from azure.storage.blob import BlobServiceClient
        except ImportError:
            logger.error("Required Azure dependencies not found. Please install them with:")
            logger.error("pip install azure-identity azure-mgmt-resource azure-mgmt-storage azure-storage-blob")
            sys.exit(1)

        credential = AzureCliCredential()
        resource_client = ResourceManagementClient(credential, subscription_id)
        storage_client = StorageManagementClient(credential, subscription_id)

        # Set az cli to look at the correct subscription
        run(f"az account set --subscription {subscription_id}")

        # Generate resource names
        resource_group_name = get_name(lfo_base_name, RESOURCE_GROUP_MAX_LENGTH)
        storage_account_name = get_name(lfo_base_name, STORAGE_ACCOUNT_MAX_LENGTH)
        container_registry_name = get_name(lfo_base_name, CONTAINER_REGISTRY_MAX_LENGTH)

        logger.info(f"Using resource group name: {resource_group_name}")
        logger.info(f"Using storage account name: {storage_account_name}")
        logger.info(f"Using container registry name: {container_registry_name}")

        # Create or update resource group
        resource_group, initial_deploy = create_or_update_resource_group(resource_client, resource_group_name, location)

        # Create or update storage account
        storage_account, connection_string = create_or_update_storage_account(
            storage_client, resource_group_name, storage_account_name, location
        )

        # Create blob service client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Create or update storage container
        create_or_update_storage_container(blob_service_client, CONTAINER_NAME)

        # Create or update container registry
        create_or_update_container_registry(resource_group_name, container_registry_name, location, lfo_dir, config)

        # Build and push Docker images
        build_and_push_docker_images(container_registry_name, lfo_dir, config)

        # Build and upload tasks
        build_and_upload_tasks(lfo_dir, storage_account_name, connection_string)

        # Deploy LFO or execute deployer job
        if initial_deploy or args.force_arm_deploy:
            deploy_lfo(
                resource_group_name,
                subscription_id,
                location,
                container_registry_name,
                storage_account_name,
                lfo_dir,
                config,
            )
        else:
            execute_deployer_job(resource_group_name, lfo_dir)

        logger.info("Deployment completed successfully!")

    except Exception as e:
        logger.error(f"Deployment failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        # Parse command line arguments
        args = parse_args()

        # Check if we should generate a sample configuration file
        if args.generate_config:
            generate_config_file(args.generate_config)
            # generate_config_file will exit the script

        # Load configuration
        config = load_config(args.config)

        # Set up logging
        logger = setup_logging(config)

        main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
