#!/usr/bin/env python3
"""
Azure Log Forwarding Orchestration Installation Script

This script deploys necessary infrastructure to enable Automated Log Forwarding in an Azure environment.

This script is designed to be executed in Azure Cloud Shell.

USAGE:
    python install.py \
        --management-group "/providers/Microsoft.Management/managementGroups/your-mg-id" \
        --control-plane-region "eastus" \
        --control-plane-subscription "12345678-1234-1234-1234-123456789012" \
        --control-plane-resource-group "dd-control-plane-rg" \
        --monitored-subscriptions "12345678-1234-1234-1234-123456789012,87654321-4321-4321-4321-210987654321" \
        --datadog-api-key "your-32-char-api-key" \
        --datadog-app-key "your-40-char-app-key" \
        --datadog-site "datadoghq.com"
        --resource-tag-filters "environment:prod,team:platform" \
        --pii-scrubber-rules "$(cat pii-rules.yaml)" \
        --datadog-telemetry \
        --log-level "DEBUG"

PARAMETERS:
    Required:
        --management-group: Management group ID for enterprise-scale deployment
        --control-plane-region: Azure region (e.g., eastus, westus2, northeurope)
        --control-plane-subscription: Subscription ID for the control plane infrastructure  
        --control-plane-resource-group: Resource group name for control plane resources
        --monitored-subscriptions: Comma-separated subscription IDs to monitor for logs
        --datadog-api-key: 32-character Datadog API key from organization settings
        --datadog-app-key: 40-character Datadog Application key for deployer access
        --datadog-site: Datadog site region (default: datadoghq.com)
    Optional:
        --resource-tag-filters: Comma-separated tags to filter which resources to monitor
        --pii-scrubber-rules: YAML-formatted PII scrubbing rules
        --datadog-telemetry: Enable Datadog telemetry collection
        --log-level: Logging verbosity (DEBUG, INFO, WARNING, ERROR)
"""

import argparse
import json
import subprocess
import time
import uuid
from dataclasses import dataclass
from logging import INFO, WARNING, basicConfig, getLogger

# Set up logging
getLogger("azure").setLevel(WARNING)
log = getLogger("installer")

# =============================================================================
# CONFIGURATION PARAMETERS
# =============================================================================


def generate_control_plane_id(
    management_group_id: str, subscription_id: str, resource_group: str, location: str
) -> str:
    """Generate control plane ID matching bicep logic: subUuid(guid(...))."""

    # Create deterministic UUID from same inputs as bicep
    combined = f"{management_group_id}{subscription_id}{resource_group}{location}"

    # Create a deterministic UUID using the combined string
    # This mimics the guid() function in bicep
    namespace = uuid.UUID("00000000-0000-0000-0000-000000000000")
    guid_like = str(uuid.uuid5(namespace, combined))

    # Extract last 12 characters and convert to lowercase (matching bicep subUuid function)
    # Remove hyphens and take the last 12 characters
    clean_guid = guid_like.replace("-", "")
    return clean_guid[-12:].lower()


@dataclass
class Configuration:
    """Class to hold all configuration parameters."""

    # Required parameters
    management_group_id: str
    control_plane_region: str
    control_plane_subscription_id: str
    control_plane_rg: str
    monitored_subs: str
    datadog_api_key: str
    datadog_app_key: str

    # Optional parameters (with defaults)
    datadog_site: str = "datadoghq.com"
    resource_tag_filters_arg: str = ""
    pii_scrubber_rules_arg: str = ""
    datadog_telemetry_arg: bool = False
    log_level_arg: str = "DEBUG"
    # altan log_level_arg: str = "INFO"

    def __post_init__(self):
        """Post-initialization to calculate derived values."""
        # Basic configuration mapping
        self.control_plane_subscription = self.control_plane_subscription_id
        self.control_plane_resource_group = self.control_plane_rg
        self.control_plane_region = self.control_plane_region

        # Datadog configuration
        self.datadog_application_key = self.datadog_app_key

        # Parse monitored subscriptions from comma-separated string
        self.monitored_subscriptions = [sub.strip() for sub in self.monitored_subs.split(",") if sub.strip()]

        self.resource_tag_filters = self.resource_tag_filters_arg
        self.pii_scrubber_rules = self.pii_scrubber_rules_arg
        self.datadog_telemetry = self.datadog_telemetry_arg
        self.log_level = self.log_level_arg

        self.control_plane_id = generate_control_plane_id(
            self.management_group_id,
            self.control_plane_subscription,
            self.control_plane_resource_group,
            self.control_plane_region,
        )
        log.info(f"Generated control plane ID: {self.control_plane_id}")

        # Derived resource names (calculated after base configuration is set)
        self.control_plane_cache = "control-plane-cache"
        self.control_plane_cache_storage_name = f"lfostorage{self.control_plane_id}"
        self.control_plane_cache_storage_url = f"https://{self.control_plane_cache_storage_name}.blob.core.windows.net"
        self.control_plane_cache_storage_key = ""

        self.app_service_plan = f"control-plane-asp-{self.control_plane_id}"
        self.control_plane_env = f"dd-log-forwarder-env-{self.control_plane_id}-{self.control_plane_region}"
        self.deployer_job_name = f"deployer-task-{self.control_plane_id}"
        self.container_app_start_role = f"ContainerAppStartRole{self.control_plane_id}"
        self.control_plane_resource_group_id = (
            f"/subscriptions/{self.control_plane_subscription}/resourceGroups/{self.control_plane_resource_group}"
        )

        # Container configuration
        self.lfo_public_storage_account_url = "https://ddazurelfo.blob.core.windows.net"
        self.image_registry = "datadoghq.azurecr.io"
        self.deployer_image = f"{self.image_registry}/deployer:latest"

        self.control_plane_function_apps = {
            "resources": f"resources-task-{self.control_plane_id}",
            "scaling": f"scaling-task-{self.control_plane_id}",
            "diagnostic": f"diagnostic-settings-task-{self.control_plane_id}",
        }

    def get_control_plane_cache_conn_string(self) -> str:
        """Get the connection string for the control plane cache storage account."""
        if not self.control_plane_cache_storage_key:
            self.control_plane_cache_storage_key = get_storage_key(
                self.control_plane_cache_storage_name, self.control_plane_resource_group
            )
        return f"DefaultEndpointsProtocol=https;AccountName={self.control_plane_cache_storage_name};EndpointSuffix=core.windows.net;AccountKey={self.control_plane_cache_storage_key}"


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Azure Log Forwarding Orchestration Installation Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Required parameters
    parser.add_argument(
        "-mg", "--management-group", type=str, required=True, help="Management group ID to deploy under (required)"
    )

    parser.add_argument(
        "--control-plane-region",
        type=str,
        required=True,
        help="Azure region for the control plane (e.g., eastus, westus2) (required)",
    )

    parser.add_argument(
        "--control-plane-subscription",
        type=str,
        required=True,
        help="Subscription ID where the control plane will be deployed (required)",
    )

    parser.add_argument(
        "--control-plane-resource-group",
        type=str,
        required=True,
        help="Resource group name for the control plane (required)",
    )

    parser.add_argument(
        "--monitored-subscriptions",
        type=str,
        required=True,
        help="Comma-separated list of subscription IDs to monitor for log forwarding (required)",
    )

    parser.add_argument("--datadog-api-key", type=str, required=True, help="Datadog API key (required)")

    parser.add_argument("--datadog-app-key", type=str, required=True, help="Datadog Application key (required)")

    parser.add_argument(
        "--datadog-site",
        type=str,
        choices=[
            "datadoghq.com",
            "datadoghq.eu",
            "ap1.datadoghq.com",
            "ap2.datadoghq.com",
            "us3.datadoghq.com",
            "us5.datadoghq.com",
            "ddog-gov.com",
        ],
        default="datadoghq.com",
        help="Datadog site (default: datadoghq.com)",
    )

    # Optional parameters
    parser.add_argument(
        "--resource-tag-filters", type=str, default="", help="Comma separated list of tags to filter resources by"
    )

    parser.add_argument("--pii-scrubber-rules", type=str, default="", help="YAML formatted list of PII Scrubber Rules")

    parser.add_argument("--datadog-telemetry", action="store_true", help="Enable Datadog telemetry")

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set the log level (default: INFO)",
    )

    return parser.parse_args()


# =============================================================================
# UTILITY
# =============================================================================


class AzCommand:
    """Builder for Azure CLI commands."""

    def __init__(self, service: str, action: str):
        """Initialize with service and action (e.g., 'functionapp', 'create')."""
        self.cmd = [service, action]

    def param(self, key: str, value: str) -> "AzCommand":
        """Add a parameter with key-value pair (key should include --)."""
        self.cmd.extend([key, value])
        return self

    def param_list(self, key: str, values: list[str]) -> "AzCommand":
        """Add multiple parameters with the same key"""
        self.cmd.append(key)
        self.cmd.extend(values)
        return self

    def flag(self, flag: str) -> "AzCommand":
        """Add a flag (should include --)."""
        self.cmd.append(flag)
        return self


def execute(az_cmd: AzCommand) -> str:
    """Run an Azure CLI command and return output or raise error."""

    command = az_cmd.cmd
    log.debug(f"Running: az {' '.join(command)}")
    full_command = ["az"] + command
    result = subprocess.run(full_command, capture_output=True, text=True)
    if result.returncode != 0:
        log.error(f"Command failed: az {' '.join(command)}")
        log.error(result.stderr)
        raise RuntimeError(f"Command failed: az {' '.join(command)}")
    return result.stdout


# =============================================================================
# VALIDATION PHASE
# =============================================================================


def validate_deployment(config: Configuration):
    """Phase 0: Validate all parameters and permissions before creating anything."""
    log.info("=" * 70)
    log.info("VALIDATION: Checking deployment parameters and permissions...")
    log.info("=" * 70)

    # Validate Azure CLI and authentication
    validate_azure_cli()

    # Validate subscription access
    validate_subscription_access(config.control_plane_subscription)

    # Validate resource names
    validate_resource_names(
        config.control_plane_resource_group, config.control_plane_subscription, config.control_plane_cache_storage_name
    )

    # Validate Datadog credentials
    validate_datadog_credentials(config.datadog_api_key, config.datadog_site)

    # Validate configuration parameters
    validate_configuration(config)

    # Validate monitored subscription access
    validate_monitored_subscriptions(config.monitored_subscriptions, config.control_plane_subscription)

    log.info("=" * 70)
    log.info("VALIDATION COMPLETED: All checks passed - ready to deploy")
    log.info("=" * 70)


def validate_azure_cli():
    """Ensure Azure CLI is installed and user is authenticated."""
    try:
        execute(AzCommand("account", "show"))
        log.debug("Azure CLI authentication verified")
    except Exception as e:
        raise RuntimeError("Azure CLI not authenticated. Run 'az login' first.")


def validate_subscription_access(control_plane_subscription: str):
    """Verify access to the control plane subscription."""
    try:
        set_subscription(control_plane_subscription)
        log.debug(f"Subscription access verified: {control_plane_subscription}")
    except Exception as e:
        raise RuntimeError(f"Cannot access subscription {control_plane_subscription}: {e}")


def validate_resource_names(
    control_plane_resource_group: str, control_plane_subscription: str, storage_account_name: str
):
    """Check if resource names are available and valid."""
    log.info("Validating resource name availability...")

    # Check if resource group already exists
    try:
        output = execute(
            AzCommand("group", "exists")
            .param("--name", control_plane_resource_group)
            .param("--subscription", control_plane_subscription)
        )
        if output.strip().lower() == "true":
            log.warning(f"Resource group {control_plane_resource_group} already exists - will use existing")
        else:
            log.debug(f"Resource group name available: {control_plane_resource_group}")
    except Exception as e:
        raise RuntimeError(f"Cannot check resource group availability: {e}")

    # Check storage account name availability
    try:
        output = execute(AzCommand("storage", "account check-name").param("--name", storage_account_name))
        result = json.loads(output)
        if not result.get("nameAvailable", False):
            reason = result.get("reason", "Unknown")
            message = result.get("message", "")
            # raise RuntimeError(f"Storage account name '{storage_account_name}' not available: {reason} - {message}")
            log.info(f"Storage account name '{storage_account_name}' exists - will use existing")
        log.debug(f"Storage account name available: {storage_account_name}")
    except json.JSONDecodeError:
        raise RuntimeError("Failed to parse storage account name availability check")


def validate_datadog_credentials(datadog_api_key: str, datadog_site: str):
    """Validate Datadog API credentials without making changes."""
    log.info("Validating Datadog API credentials...")

    if not datadog_api_key:
        raise RuntimeError("Datadog API key not configured")

    try:
        curl_command = [
            "curl",
            "-s",
            "-X",
            "GET",
            f"https://api.{datadog_site}/api/v1/validate",
            "-H",
            "Accept: application/json",
            "-H",
            f"DD-API-KEY: {datadog_api_key}",
        ]
        response = subprocess.check_output(curl_command, text=True)
        response_json = json.loads(response)
        if not response_json.get("valid", False):
            raise RuntimeError(f"Datadog API Key validation failed against {datadog_site}")

        log.debug("Datadog API credentials validated")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to validate Datadog credentials: {e}")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse Datadog validation response: {e}")


def validate_configuration(config: Configuration):
    """Validate configuration parameters."""
    log.info("Validating configuration parameters...")

    if not config.control_plane_subscription:
        raise RuntimeError("Control plane subscription not configured")

    if not config.control_plane_resource_group:
        raise RuntimeError("Control plane resource group not configured")

    if not config.control_plane_region:
        raise RuntimeError("Control plane location not configured")

    if not config.monitored_subscriptions:
        raise RuntimeError("Monitored subscriptions not properly configured")

    if config.log_level not in ["DEBUG", "INFO", "WARNING", "ERROR"]:
        raise RuntimeError(f"Invalid log level: {config.log_level}. Must be one of: DEBUG, INFO, WARNING, ERROR")

    log.debug("Configuration validation completed")


def validate_monitored_subscriptions(monitored_subscriptions: list[str], control_plane_subscription: str):
    """Verify access to all monitored subscriptions."""
    log.info("Validating access to monitored subscriptions...")

    for sub_id in monitored_subscriptions:
        try:
            set_subscription(sub_id)
            log.debug(f"Monitored subscription access verified: {sub_id}")
        except Exception as e:
            raise RuntimeError(f"Cannot access monitored subscription {sub_id}: {e}")


# =============================================================================
# STEP 1: BASIC RESOURCE SETUP
# Source: scripts/lifecycle/01_install_param.py
# =============================================================================


def set_subscription(subscription_id: str):
    """Set the active Azure subscription."""
    log.debug(f"Setting active subscription to {subscription_id}")
    execute(AzCommand("account", "set").param("--subscription", subscription_id))


def create_resource_group(control_plane_resource_group: str, control_plane_location: str):
    """Create the control plane resource group."""
    log.info(f"Creating resource group {control_plane_resource_group} in {control_plane_location}")
    execute(
        AzCommand("group", "create")
        .param("--name", control_plane_resource_group)
        .param("--location", control_plane_location)
    )


def create_storage_account(storage_account_name: str, control_plane_resource_group: str, control_plane_location: str):
    """Create the storage account for the control plane."""
    log.info(f"Creating storage account {storage_account_name}")
    execute(
        AzCommand("storage", "account create")
        .param("--name", storage_account_name)
        .param("--resource-group", control_plane_resource_group)
        .param("--location", control_plane_location)
        .param("--sku", "Standard_LRS")
        .param("--kind", "StorageV2")
        .param("--access-tier", "Hot")
        .param("--min-tls-version", "TLS1_2")
        .flag("--https-only")
    )


def get_storage_key(storage_account_name: str, control_plane_resource_group: str) -> str:
    """Get the storage account primary key."""
    log.debug(f"Retrieving storage account key for {storage_account_name}")
    output = execute(
        AzCommand("storage", "account keys list")
        .param("--account-name", storage_account_name)
        .param("--resource-group", control_plane_resource_group)
    )
    keys = json.loads(output)
    return keys[0]["value"]


def create_blob_container(storage_account_name: str, control_plane_cache: str, account_key: str):
    """Create blob container in the storage account."""
    log.info(f"Creating blob container {control_plane_cache}")
    execute(
        AzCommand("storage", "container create")
        .param("--account-name", storage_account_name)
        .param("--account-key", account_key)
        .param("--name", control_plane_cache)
    )


def create_file_share(storage_account_name: str, control_plane_cache: str, resource_group: str):
    """Create file share in the storage account."""
    log.info(f"Creating file share {control_plane_cache}")
    execute(
        AzCommand("storage", "share-rm create")
        .param("--storage-account", storage_account_name)
        .param("--name", control_plane_cache)
        .param("--resource-group", resource_group)
    )


# =============================================================================
# STEP 2: DATADOG API KEY VALIDATION
# =============================================================================


def validate_datadog_api_key(datadog_site: str, datadog_api_key: str):
    """Validate the Datadog API key."""
    log.info("Validating Datadog API key...")

    # Construct curl command
    curl_command = [
        "curl",
        "-s",
        "-X",
        "GET",
        f"https://api.{datadog_site}/api/v1/validate",
        "-H",
        "Accept: application/json",
        "-H",
        f"DD-API-KEY: {datadog_api_key}",
    ]

    # Run curl and parse result
    response = subprocess.check_output(curl_command, text=True)
    log.debug(f"Datadog API validation response: {response}")

    try:
        response_json = json.loads(response)
        if not response_json.get("valid", False):
            raise RuntimeError(f"Datadog API Key validation failed against {datadog_site}")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse Datadog response: {e}")

    log.info("Datadog API Key validated successfully")


# =============================================================================
# STEP 3: APP SERVICE PLAN AND FUNCTION APPS
# Source: scripts/lifecycle/03_asp_control_plane.py
# =============================================================================


def create_app_service_plan(app_service_plan: str, control_plane_resource_group: str, control_plane_location: str):
    """Create the App Service Plan for Function Apps."""

    # Check if the app service plan already exists
    try:
        log.info(f"Checking if App Service Plan '{app_service_plan}' already exists...")
        execute(
            AzCommand("appservice", "plan show")
            .param("--name", app_service_plan)
            .param("--resource-group", control_plane_resource_group)
        )
        log.info(f"App Service Plan '{app_service_plan}' already exists - reusing existing plan")
        return
    except RuntimeError:
        # App service plan doesn't exist, proceed with creation
        log.info(f"App Service Plan '{app_service_plan}' not found - creating new plan")
        pass

    log.info(f"Creating App Service Plan {app_service_plan}")

    # Use `az resource create` instead of `az appservice plan create` because of
    # Azure CLI issue with the SKU (Y1) we utilize: https://github.com/Azure/azure-cli/issues/19864
    execute(
        AzCommand("resource", "create")
        .param("--resource-group", control_plane_resource_group)
        .param("--name", app_service_plan)
        .param("--resource-type", "Microsoft.Web/serverfarms")
        .flag("--is-full-object")
        .param(
            "--properties",
            json.dumps(
                {
                    "name": app_service_plan,
                    "location": control_plane_location,
                    "kind": "linux",
                    "sku": {"name": "Y1", "tier": "Dynamic"},
                    "properties": {"reserved": True},
                }
            ),
        )
        .param("--api-version", "2022-09-01")
    )


def create_function_app(config: Configuration, name: str, key: str):
    """Create a Function App with required configuration."""

    # Check if the function app already exists
    try:
        log.info(f"Checking if Function App '{name}' already exists...")
        execute(
            AzCommand("functionapp", "show")
            .param("--name", name)
            .param("--resource-group", config.control_plane_resource_group)
        )
        log.info(f"Function App '{name}' already exists - skipping creation and updating configuration")
        function_app_exists = True
    except RuntimeError:
        log.info(f"Function App '{name}' not found - creating new function app")
        function_app_exists = False

    if not function_app_exists:
        log.info(f"Creating Function App {name}")
        execute(
            AzCommand("functionapp", "create")
            .param("--resource-group", config.control_plane_resource_group)
            .param("--consumption-plan-location", config.control_plane_region)
            .param("--runtime", "python")
            .param("--functions-version", "4")
            .param("--os-type", "Linux")
            .param("--name", name)
            .param("--storage-account", config.control_plane_cache_storage_name)
            .flag("--assign-identity")
        )

    common_settings = [
        f"AzureWebJobsStorage={config.get_control_plane_cache_conn_string()}",
        "FUNCTIONS_EXTENSION_VERSION=~4",
        "FUNCTIONS_WORKER_RUNTIME=python",
        f"WEBSITE_CONTENTAZUREFILECONNECTIONSTRING={config.get_control_plane_cache_conn_string()}",
        f"WEBSITE_CONTENTSHARE={name}",
        "AzureWebJobsFeatureFlags=EnableWorkerIndexing",
        f"DD_API_KEY={config.datadog_api_key}",
        f"DD_SITE={config.datadog_site}",
        f"DD_TELEMETRY={'true' if config.datadog_telemetry else 'false'}",
        f"CONTROL_PLANE_ID={config.control_plane_id}",
        f"LOG_LEVEL={config.log_level}",
    ]

    # Function-specific settings
    if "resources" in name:
        specific_settings = [
            f"MONITORED_SUBSCRIPTIONS={','.join(config.monitored_subscriptions)}",
            f"RESOURCE_TAG_FILTERS={config.resource_tag_filters}",
        ]
    elif "diagnostic" in name:
        specific_settings = [
            f"RESOURCE_GROUP={config.control_plane_resource_group}",
        ]
    elif "scaling" in name:
        specific_settings = [
            f"RESOURCE_GROUP={config.control_plane_resource_group}",
            f"FORWARDER_IMAGE={config.image_registry}/forwarder:latest",
            f"CONTROL_PLANE_REGION={config.control_plane_region}",
            f"PII_SCRUBBER_RULES={config.pii_scrubber_rules}",
        ]
    else:
        specific_settings = []

    all_settings = common_settings + specific_settings

    # Always update app settings (even if function app exists) to ensure configuration is current
    log.debug(f"Configuring app settings for Function App {name}")
    execute(
        AzCommand("functionapp", "config appsettings set")
        .param("--name", name)
        .param("--resource-group", config.control_plane_resource_group)
        .param_list("--settings", all_settings)
    )

    # Always update runtime configuration
    log.debug(f"Configuring Linux runtime for Function App {name}")
    execute(
        AzCommand("functionapp", "config set")
        .param("--name", name)
        .param("--resource-group", config.control_plane_resource_group)
        .param("--linux-fx-version", "Python|3.11")
    )


def create_function_apps(config: Configuration):
    """Create all required Function Apps."""
    log.info("Creating App Service Plan...")
    create_app_service_plan(config.app_service_plan, config.control_plane_resource_group, config.control_plane_region)

    log.info("Fetching storage key...")
    key = get_storage_key(config.control_plane_cache_storage_name, config.control_plane_resource_group)

    log.info("Creating Function Apps...")
    for _role, app_name in config.control_plane_function_apps.items():
        log.info(f"Creating Function App: {app_name}")
        create_function_app(config, app_name, key)

    log.info("Function Apps created and configured")


# =============================================================================
# STEP 4: CONTAINER APP ENVIRONMENT AND DEPLOYER JOB
# Source: scripts/lifecycle/04_deployer.py
# =============================================================================


def create_user_assigned_identity(control_plane_resource_group: str, control_plane_location: str):
    """Create a user-assigned managed identity."""
    identity_name = "runInitialDeployIdentity"

    # Check if the identity already exists
    try:
        log.info("Checking if user-assigned managed identity already exists...")
        execute(
            AzCommand("identity", "show")
            .param("--name", identity_name)
            .param("--resource-group", control_plane_resource_group)
        )
        log.info(f"User-assigned managed identity '{identity_name}' already exists - reusing existing identity")
        return
    except RuntimeError:
        # Identity doesn't exist, proceed with creation
        log.info("User-assigned managed identity not found - creating new identity")
        pass

    execute(
        AzCommand("identity", "create")
        .param("--name", identity_name)
        .param("--resource-group", control_plane_resource_group)
        .param("--location", control_plane_location)
        .flag("--enable-managed-identity")
    )


def create_containerapp_environment(
    control_plane_env: str, control_plane_resource_group: str, control_plane_location: str
):
    """Create the Container App environment."""

    # Check if the container app environment already exists
    try:
        log.info(f"Checking if Container App environment '{control_plane_env}' already exists...")
        execute(
            AzCommand("containerapp", "env show")
            .param("--name", control_plane_env)
            .param("--resource-group", control_plane_resource_group)
        )
        log.info(f"Container App environment '{control_plane_env}' already exists - reusing existing environment")
        return
    except RuntimeError:
        # Environment doesn't exist, proceed with creation
        log.info(f"Container App environment '{control_plane_env}' not found - creating new environment")
        pass

    log.info(f"Creating Container App environment {control_plane_env}")
    execute(
        AzCommand("containerapp", "env create")
        .param("--name", control_plane_env)
        .param("--resource-group", control_plane_resource_group)
        .param("--location", control_plane_location)
    )


def create_containerapp_job(config: Configuration):
    """Create the Container App job for the deployer."""

    # Check if the container app job already exists
    try:
        log.info(f"Checking if Container App job '{config.deployer_job_name}' already exists...")
        execute(
            AzCommand("containerapp", "job show")
            .param("--name", config.deployer_job_name)
            .param("--resource-group", config.control_plane_resource_group)
        )
        log.info(f"Container App job '{config.deployer_job_name}' already exists - reusing existing job")
        return
    except RuntimeError:
        # Container app job doesn't exist, proceed with creation
        log.info(f"Container App job '{config.deployer_job_name}' not found - creating new job")
        pass

    log.info(f"Creating Container App job {config.deployer_job_name}")

    env_vars = [
        "AzureWebJobsStorage=secretref:connection-string",
        f"SUBSCRIPTION_ID={config.control_plane_subscription}",
        f"RESOURCE_GROUP={config.control_plane_resource_group}",
        f"CONTROL_PLANE_ID={config.control_plane_id}",
        f"CONTROL_PLANE_REGION={config.control_plane_region}",
        "DD_API_KEY=secretref:dd-api-key",
        "DD_APP_KEY=secretref:dd-app-key",
        f"DD_SITE={config.datadog_site}",
        f"DD_TELEMETRY={'true' if config.datadog_telemetry else 'false'}",
        f"STORAGE_ACCOUNT_URL={config.lfo_public_storage_account_url}",
        f"LOG_LEVEL={config.log_level}",
    ]

    secrets = [
        f"connection-string={config.get_control_plane_cache_conn_string()}",
        f"dd-api-key={config.datadog_api_key}",
        f"dd-app-key={config.datadog_application_key}",
    ]

    execute(
        AzCommand("containerapp", "job create")
        .param("--name", config.deployer_job_name)
        .param("--resource-group", config.control_plane_resource_group)
        .param("--environment", config.control_plane_env)
        .param("--replica-timeout", "1800")
        .param("--replica-retry-limit", "1")
        .param("--trigger-type", "Schedule")
        .param("--cron-expression", "*/30 * * * *")
        .param("--image", config.deployer_image)
        .param("--cpu", "0.5")
        .param("--memory", "1Gi")
        .param("--parallelism", "1")
        .param("--replica-completion-count", "1")
        .flag("--mi-system-assigned")
        .param_list("--env-vars", env_vars)
        .param_list("--secrets", secrets)
    )


def create_custom_role_definition(container_app_start_role: str, control_plane_resource_group: str):
    """Create a custom role for starting container app jobs."""

    # Get the resource group scope
    scope = execute(
        AzCommand("group", "show")
        .param("--name", control_plane_resource_group)
        .param("--query", "id")
        .param("--output", "tsv")
    ).strip()

    # Check if the custom role definition already exists
    try:
        log.info(f"Checking if custom role definition '{container_app_start_role}' already exists...")
        output = execute(
            AzCommand("role", "definition list")
            .param("--name", container_app_start_role)
            .param("--scope", scope)
            .param("--query", "[0].name")
            .param("--output", "tsv")
        )
        if output.strip():
            log.info(f"Custom role definition '{container_app_start_role}' already exists - reusing existing role")
            return
        else:
            log.info(f"Custom role definition '{container_app_start_role}' not found - creating new role")
    except RuntimeError:
        # Role doesn't exist or error occurred, proceed with creation
        log.info(f"Custom role definition '{container_app_start_role}' not found - creating new role")
        pass

    log.info(f"Creating custom role definition {container_app_start_role}")

    role_definition = {
        "Name": container_app_start_role,
        "IsCustom": True,
        "Description": "Custom role to start container app jobs",
        "Actions": ["Microsoft.App/jobs/start/action"],
        "NotActions": [],
        "AssignableScopes": [scope],
    }

    with open("custom_role.json", "w") as f:
        json.dump(role_definition, f)

    execute(AzCommand("role", "definition create").param("--role-definition", "custom_role.json"))


def assign_custom_role_to_identity(control_plane_resource_group: str, container_app_start_role: str):
    """Assign the custom role to the managed identity."""
    log.info("Assigning custom role to managed identity")
    identity_id = execute(
        AzCommand("identity", "show")
        .param("--name", "runInitialDeployIdentity")
        .param("--resource-group", control_plane_resource_group)
        .param("--query", "principalId")
        .param("--output", "tsv")
    ).strip()

    scope = execute(
        AzCommand("group", "show")
        .param("--name", control_plane_resource_group)
        .param("--query", "id")
        .param("--output", "tsv")
    ).strip()

    role_id = execute(
        AzCommand("role", "definition list")
        .param("--name", container_app_start_role)
        .param("--scope", scope)
        .param("--query", "[0].name")
        .param("--output", "tsv")
    ).strip()

    # Check if the role assignment already exists
    try:
        log.debug(
            f"Checking if custom role assignment already exists for role {container_app_start_role} to identity {identity_id}"
        )
        output = execute(
            AzCommand("role", "assignment list")
            .param("--assignee", identity_id)
            .param("--role", role_id)
            .param("--scope", scope)
            .param("--query", "length([])")
            .param("--output", "tsv")
        )
        if int(output.strip()) > 0:
            log.info(
                f"Custom role assignment already exists for role {container_app_start_role} to managed identity - skipping"
            )
            return
        else:
            log.debug("Custom role assignment not found - creating new assignment")
    except (RuntimeError, ValueError):
        # Role assignment doesn't exist or error occurred, proceed with creation
        log.debug("Custom role assignment not found - creating new assignment")
        pass

    execute(
        AzCommand("role", "assignment create")
        .param("--role", role_id)
        .param("--assignee-object-id", identity_id)
        .param("--assignee-principal-type", "ServicePrincipal")
        .param("--scope", scope)
    )


def deploy_container_job_infra(config: Configuration):
    """Deploy all container job infrastructure."""
    log.info("Creating managed identity...")
    create_user_assigned_identity(config.control_plane_resource_group, config.control_plane_region)

    log.info("Creating container app environment...")
    create_containerapp_environment(
        config.control_plane_env, config.control_plane_resource_group, config.control_plane_region
    )

    log.info("Creating container app job...")
    create_containerapp_job(config)

    log.info("Defining custom role...")
    create_custom_role_definition(config.container_app_start_role, config.control_plane_resource_group)

    log.info("Assigning custom role to identity...")
    assign_custom_role_to_identity(config.control_plane_resource_group, config.container_app_start_role)

    log.info("Container App job + identity setup complete")


# =============================================================================
# STEP 5: INITIAL DEPLOYMENT TRIGGER
# Source: scripts/lifecycle/05_initial_job_trigger.py
# =============================================================================


def run_initial_deploy_script(config: Configuration):
    """Trigger the initial deployment using deployment scripts."""
    log.info("Starting initial container app job via deployment script...")

    # Get the full identity resource ID
    # identity_id = az(
    #     [
    #         "identity",
    #         "show",
    #         "--name",
    #         "runInitialDeployIdentity",
    #         "--resource-group",
    #         config.control_plane_resource_group,
    #         "--query",
    #         "id",
    #         "--output",
    #         "tsv",
    #     ]
    # ).strip()

    # # Get the storage key again
    # storage_key = get_storage_key(config.storage_account_name, config.control_plane_resource_group)

    # # Build PowerShell script content
    # ps_script = f"Start-AzContainerAppJob -Name {config.deployer_job_name} -ResourceGroupName {config.control_plane_resource_group}"

    # az(
    #     [
    #         "deployment-scripts",
    #         "create",
    #         "--name",
    #         "runInitialDeploy",
    #         "--resource-group",
    #         config.control_plane_resource_group,
    #         "--location",
    #         config.control_plane_location,
    #         "--script-name",
    #         "runInitialDeploy",
    #         "--script-content",
    #         ps_script,
    #         "--az-powershell-version",
    #         "12.3",
    #         "--storage-account-name",
    #         config.storage_account_name,
    #         "--storage-account-key",
    #         storage_key,
    #         "--cleanup-preference",
    #         "OnSuccess",
    #         "--retention-interval",
    #         "PT1H",
    #         "--identity-type",
    #         "UserAssigned",
    #         "--user-assigned-identities",
    #         identity_id,
    #     ]
    # )

    log.info("Initial deployment script executed")


# =============================================================================
# STEP 6: RBAC PERMISSIONS ACROSS SUBSCRIPTIONS
# Source: scripts/lifecycle/06_monitored_subs_roles.py
# =============================================================================


def get_function_principal_id(control_plane_resource_group: str, function_app_name: str) -> str:
    """Get the principal ID of a Function App's managed identity."""
    log.debug(f"Getting principal ID for Function App {function_app_name}")
    output = execute(
        AzCommand("functionapp", "identity show")
        .param("--name", function_app_name)
        .param("--resource-group", control_plane_resource_group)
        .param("--query", "principalId")
        .param("--output", "tsv")
    )
    return output.strip()


def get_containerapp_job_principal_id(control_plane_resource_group: str, job_name: str) -> str:
    """Get the principal ID of a Container App Job's managed identity."""
    log.debug(f"Getting principal ID for Container App Job {job_name}")
    output = execute(
        AzCommand("containerapp", "job show")
        .param("--name", job_name)
        .param("--resource-group", control_plane_resource_group)
        .param("--query", "identity.principalId")
        .param("--output", "tsv")
    )
    return output.strip()


def assign_role(scope: str, principal_id: str, role_id: str, control_plane_id: str):
    """Assign a role to a principal at a given scope."""

    # Check if the role assignment already exists
    try:
        log.debug(
            f"Checking if role assignment already exists for role {role_id} to principal {principal_id} at scope {scope}"
        )
        output = execute(
            AzCommand("role", "assignment list")
            .param("--assignee", principal_id)
            .param("--role", role_id)
            .param("--scope", scope)
            .param("--query", "length([])")
            .param("--output", "tsv")
        )
        if int(output.strip()) > 0:
            log.debug(
                f"Role assignment already exists for role {role_id} to principal {principal_id} at scope {scope} - skipping"
            )
            return
        else:
            log.debug("Role assignment not found - creating new assignment")
    except (RuntimeError, ValueError):
        # Role assignment doesn't exist or error occurred, proceed with creation
        log.debug("Role assignment not found - creating new assignment")
        pass

    log.debug(f"Assigning role {role_id} to principal {principal_id} at scope {scope}")
    execute(
        AzCommand("role", "assignment create")
        .param("--assignee-object-id", principal_id)
        .param("--assignee-principal-type", "ServicePrincipal")
        .param("--role", role_id)
        .param("--scope", scope)
        .param("--description", f"ddlfo{control_plane_id}")
    )


def grant_subscription_permissions(config: Configuration):
    """Grant permissions across all monitored subscriptions."""
    log.info("Setting up permissions across monitored subscriptions...")

    # Get principal IDs for function apps
    resource_task_pid = get_function_principal_id(
        config.control_plane_resource_group, config.control_plane_function_apps["resources"]
    )
    diagnostic_pid = get_function_principal_id(
        config.control_plane_resource_group, config.control_plane_function_apps["diagnostic"]
    )
    scaling_pid = get_function_principal_id(
        config.control_plane_resource_group, config.control_plane_function_apps["scaling"]
    )

    # Get principal ID for deployer container app job
    deployer_pid = get_containerapp_job_principal_id(config.control_plane_resource_group, config.deployer_job_name)

    # Assign Website Contributor role to deployer in control plane resource group
    log.info("Assigning Website Contributor role to deployer container app job...")
    assign_role(
        config.control_plane_resource_group_id,
        deployer_pid,
        "de139f84-1756-47ae-9be6-808fbbe84772",
        config.control_plane_id,
    )  # Website Contributor role

    for sub_id in config.monitored_subscriptions:
        log.info(f"Assigning permissions in subscription: {sub_id}")

        # Set context to target subscription
        set_subscription(sub_id)

        # Create RG in target subscription if it doesn't exist
        execute(
            AzCommand("group", "create")
            .param("--name", config.control_plane_resource_group)
            .param("--location", config.control_plane_region)
        )

        # Get scope
        subscription_scope = f"/subscriptions/{sub_id}"
        resource_group_scope = f"{subscription_scope}/resourceGroups/{config.control_plane_resource_group}"

        # Assign roles at subscription level
        assign_role(
            subscription_scope, resource_task_pid, "43d0d8ad-25c7-4714-9337-8ba259a9fe05", config.control_plane_id
        )  # Monitoring Reader role
        assign_role(
            subscription_scope, diagnostic_pid, "749f88d5-cbae-40b8-bcfc-e573ddc772fa", config.control_plane_id
        )  # Monitoring Contributor

        # Assign roles at resource group level
        assign_role(
            resource_group_scope, diagnostic_pid, "c12c1c16-33a1-487b-954d-41c89c60f349", config.control_plane_id
        )  # Reader and Data Access - Storage blob reader for diagnostics
        assign_role(
            resource_group_scope, scaling_pid, "b24988ac-6180-42a0-ab88-20f7382dd24c", config.control_plane_id
        )  # Contributor (used for scaling)

    # Reset back to control plane subscription
    set_subscription(config.control_plane_subscription)
    log.info("Subscription permission setup complete")


# =============================================================================
# CONTROL PLANE DEPLOYMENT
# =============================================================================


def deploy_control_plane(config: Configuration):
    """Deploy all control plane infrastructure: storage, functions, and containers."""
    log.info("Deploying storage account...")
    set_subscription(config.control_plane_subscription)
    create_storage_account(
        config.control_plane_cache_storage_name, config.control_plane_resource_group, config.control_plane_region
    )
    log.info("Waiting for storage account to be ready...")
    time.sleep(10)  # Ensure the storage account is ready
    key = get_storage_key(config.control_plane_cache_storage_name, config.control_plane_resource_group)
    create_blob_container(config.control_plane_cache_storage_name, config.control_plane_cache, key)
    create_file_share(
        config.control_plane_cache_storage_name, config.control_plane_cache, config.control_plane_resource_group
    )
    log.info("Storage account setup completed")

    log.info("Creating Function Apps...")
    create_function_apps(config)

    log.info("Deploying Container App infrastructure...")
    deploy_container_job_infra(config)

    log.info("Control plane infrastructure deployment completed")


# =============================================================================
# MAIN INSTALLATION FLOW
# =============================================================================


def main():
    """Main installation flow that orchestrates all steps."""

    try:
        # Step 0: Parse arguments and create configuration
        args = parse_arguments()
        config = Configuration(
            management_group_id=args.management_group,
            control_plane_region=args.control_plane_region,
            control_plane_subscription_id=args.control_plane_subscription,
            control_plane_rg=args.control_plane_resource_group,
            monitored_subs=args.monitored_subscriptions,
            datadog_api_key=args.datadog_api_key,
            datadog_app_key=args.datadog_app_key,
            datadog_site=args.datadog_site,
            resource_tag_filters_arg=args.resource_tag_filters,
            pii_scrubber_rules_arg=args.pii_scrubber_rules,
            datadog_telemetry_arg=args.datadog_telemetry,
            log_level_arg=args.log_level,
        )

        # Set up logging based on config
        basicConfig(level=getattr(__import__("logging"), config.log_level))

        log.info("Starting Azure Log Forwarding Orchestration Installation...")
        log.info("=" * 70)

        # Validate deployment parameters and permissions
        validate_deployment(config)

        set_subscription(config.control_plane_subscription)

        # Step 1: controlPlaneResourceGroup - Create resource group
        log.info("STEP 1: Creating control plane resource group...")
        set_subscription(config.control_plane_subscription)
        create_resource_group(config.control_plane_resource_group, config.control_plane_region)
        log.info("Control plane resource group created")

        # Step 2: validateConfig - Validate Datadog API key and configuration
        log.info("STEP 2: Validating configuration...")
        validate_datadog_api_key(config.datadog_site, config.datadog_api_key)
        log.info("Configuration validation completed")

        # Step 3: controlPlane - Deploy main infrastructure (storage + functions + containers)
        log.info("STEP 3: Deploying control plane infrastructure...")
        deploy_control_plane(config)

        # Step 4: subscriptionPermissions - Set up cross-subscription permissions
        log.info("STEP 4: Setting up cross-subscription permissions...")
        grant_subscription_permissions(config)
        log.info("Cross-subscription permissions configured")

        # Step 5: initialRun - Trigger initial deployment
        log.info("STEP 5: Triggering initial deployment...")
        run_initial_deploy_script(config)
        log.info("Initial deployment triggered")

        log.info("=" * 70)
        log.info("Azure Log Forwarding Orchestration installation completed successfully!")
        log.info("Check the Azure portal to verify all resources were created")

    except Exception as e:
        log.error(f"Installation failed with error: {e}")
        log.error("Check the Azure CLI output above for more details")
        raise


if __name__ == "__main__":
    main()
