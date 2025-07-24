#!/usr/bin/env python3

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

"""
Azure Automated Log Forwarding Installation Script

This script deploys necessary resources to enable Automated Log Forwarding in an Azure environment and is designed to be executed in Azure Cloud Shell.

usage: install.py [-h] -mg MANAGEMENT_GROUP --control-plane-region CONTROL_PLANE_REGION --control-plane-subscription CONTROL_PLANE_SUBSCRIPTION
                  --control-plane-resource-group CONTROL_PLANE_RESOURCE_GROUP --monitored-subscriptions MONITORED_SUBSCRIPTIONS --datadog-api-key DATADOG_API_KEY
                  [--datadog-site {datadoghq.com,datadoghq.eu,ap1.datadoghq.com,ap2.datadoghq.com,us3.datadoghq.com,us5.datadoghq.com,ddog-gov.com}]
                  [--resource-tag-filters RESOURCE_TAG_FILTERS] [--pii-scrubber-rules PII_SCRUBBER_RULES] [--datadog-telemetry] [--log-level {DEBUG,INFO,WARNING,ERROR}]

options:
  -h, --help            show this help message and exit
  -mg MANAGEMENT_GROUP, --management-group MANAGEMENT_GROUP
                        Management group ID to deploy under (required)
  --control-plane-region CONTROL_PLANE_REGION
                        Azure region for the control plane (e.g., eastus, westus2) (required)
  --control-plane-subscription CONTROL_PLANE_SUBSCRIPTION
                        Subscription ID where the control plane will be deployed (required)
  --control-plane-resource-group CONTROL_PLANE_RESOURCE_GROUP
                        Resource group name for the control plane (required)
  --monitored-subscriptions MONITORED_SUBSCRIPTIONS
                        Comma-separated list of subscription IDs to monitor for log forwarding (required)
  --datadog-api-key DATADOG_API_KEY
                        Datadog API key (required)
  --datadog-site {datadoghq.com,datadoghq.eu,ap1.datadoghq.com,ap2.datadoghq.com,us3.datadoghq.com,us5.datadoghq.com,ddog-gov.com}
                        Datadog site (required,default: datadoghq.com)
  --resource-tag-filters RESOURCE_TAG_FILTERS
                        Comma separated list of tags to filter resources by
  --pii-scrubber-rules PII_SCRUBBER_RULES
                        YAML formatted list of PII Scrubber Rules
  --datadog-telemetry   Enable Datadog telemetry
  --log-level {DEBUG,INFO,WARNING,ERROR}
                        Set the log level (default: INFO)
"""

import argparse
import json
import subprocess
import time
import uuid
from dataclasses import dataclass
from logging import WARNING, basicConfig, getLogger
from re import search
from time import sleep

getLogger("azure").setLevel(WARNING)
log = getLogger("installer")


def get_storage_acct_key(storage_account_name: str, control_plane_rg: str) -> str:
    """Retrieve storage account key for control plane cache - this is needed to connect to the storage account"""
    log.debug(f"Retrieving storage account key for {storage_account_name}")
    output = execute(
        AzCmd("storage", "account keys list")
        .param("--account-name", storage_account_name)
        .param("--resource-group", control_plane_rg)
    )
    keys = json.loads(output)
    return keys[0]["value"]


# =============================================================================
# CONFIGURATION INPUT PARAMETERS
# =============================================================================


@dataclass
class Configuration:
    """User-specified configuration parameters and their derivations"""

    # Required params
    management_group_id: str
    control_plane_region: str
    control_plane_sub_id: str
    control_plane_rg: str
    monitored_subs: str
    datadog_api_key: str

    # Optional params with defaults
    datadog_site: str = "datadoghq.com"
    resource_tag_filters_arg: str = ""
    pii_scrubber_rules_arg: str = ""
    datadog_telemetry_arg: bool = False
    log_level_arg: str = "INFO"

    def generate_control_plane_id(self) -> str:
        combined = (
            f"{self.management_group_id}{self.control_plane_sub_id}{self.control_plane_rg}{self.control_plane_region}"
        )

        namespace = uuid.UUID("00000000-0000-0000-0000-000000000000")
        guid_like = str(uuid.uuid5(namespace, combined))

        clean_guid = guid_like.replace("-", "")
        return clean_guid[-12:].lower()

    def get_control_plane_cache_conn_string(self) -> str:
        if not self.control_plane_cache_storage_key:
            self.control_plane_cache_storage_key = get_storage_acct_key(
                self.control_plane_cache_storage_name, self.control_plane_rg
            )
        return f"DefaultEndpointsProtocol=https;AccountName={self.control_plane_cache_storage_name};EndpointSuffix=core.windows.net;AccountKey={self.control_plane_cache_storage_key}"

    def __post_init__(self):
        """Calculates derived values from user-specified params."""

        self.monitored_subscriptions = [sub.strip() for sub in self.monitored_subs.split(",") if sub.strip()]
        self.all_subscriptions = {self.control_plane_sub_id, *self.monitored_subscriptions}

        self.resource_tag_filters = self.resource_tag_filters_arg
        self.pii_scrubber_rules = self.pii_scrubber_rules_arg
        self.datadog_telemetry = self.datadog_telemetry_arg
        self.log_level = self.log_level_arg

        # Control plane
        self.control_plane_id = self.generate_control_plane_id()
        log.info(f"Generated control plane ID: {self.control_plane_id}")
        self.control_plane_cache = "control-plane-cache"
        self.control_plane_cache_storage_name = f"lfostorage{self.control_plane_id}"
        self.control_plane_cache_storage_url = f"https://{self.control_plane_cache_storage_name}.blob.core.windows.net"
        self.control_plane_cache_storage_key = ""
        self.control_plane_resource_group_id = (
            f"/subscriptions/{self.control_plane_sub_id}/resourceGroups/{self.control_plane_rg}"
        )

        # Deployer + function apps
        self.deployer_job_name = f"deployer-task-{self.control_plane_id}"
        self.control_plane_env = f"dd-log-forwarder-env-{self.control_plane_id}-{self.control_plane_region}"
        self.container_app_start_role = f"ContainerAppStartRole{self.control_plane_id}"
        self.image_registry = "datadoghq.azurecr.io"
        self.deployer_image = f"{self.image_registry}/deployer:latest"
        self.app_service_plan = f"control-plane-asp-{self.control_plane_id}"
        self.lfo_public_storage_account_url = "https://ddazurelfo.blob.core.windows.net"
        self.control_plane_function_apps = {
            "resources": f"resources-task-{self.control_plane_id}",
            "scaling": f"scaling-task-{self.control_plane_id}",
            "diagnostic": f"diagnostic-settings-task-{self.control_plane_id}",
        }


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
# AZ COMMAND UTILITY
# =============================================================================
AUTHORIZATION_ERROR = "AuthorizationFailed"
AZURE_THROTTLING_ERROR = "TooManyRequests"
REFRESH_TOKEN_EXPIRED_ERROR = "AADSTS700082"
RESOURCE_COLLECTION_THROTTLING_ERROR = "ResourceCollectionRequestsThrottled"
MAX_RETRIES = 7


class RateLimitExceeded(Exception):
    pass


class AuthError(Exception):
    pass


class RefreshTokenError(Exception):
    pass


class AzCmd:
    """Builder for Azure CLI commands."""

    def __init__(self, service: str, action: str):
        """Initialize with service and action (e.g., 'functionapp', 'create')."""
        self.cmd = [service] + action.split()

    def param(self, key: str, value: str) -> "AzCmd":
        """Adds a key-value pair parameter"""
        self.cmd.extend([key, value])
        return self

    def param_list(self, key: str, values: list[str]) -> "AzCmd":
        """Adds a list of parameters with the same key"""
        self.cmd.append(key)
        self.cmd.extend(values)
        return self

    def flag(self, flag: str) -> "AzCmd":
        """Adds a flag to the command"""
        self.cmd.append(flag)
        return self


def try_regex_access_error(stderr: str):
    # Sample:
    # (AuthorizationFailed) The client 'user@example.com' with object id '00000000-0000-0000-0000-000000000000'
    # does not have authorization to perform action 'Microsoft.Storage/storageAccounts/read'
    # over scope '/subscriptions/00000000-0000-0000-0000-000000000000' or the scope is invalid.
    # If access was recently granted, please refresh your credentials.

    client_match = search(r"client '([^']*)'", stderr)
    action_match = search(r"action '([^']*)'", stderr)
    scope_match = search(r"scope '([^']*)'", stderr)

    if action_match and scope_match and client_match:
        client = client_match.group(1)
        action = action_match.group(1)
        scope = scope_match.group(1)
        raise RuntimeError(f"Insufficient permissions for {client} to perform {action} on {scope}")


def execute(az_cmd: AzCmd) -> str:
    """Run an Azure CLI command and return output or raise error."""

    full_command = "az " + " ".join(az_cmd.cmd)
    log.debug(f"Running: {full_command}")
    delay = 2  # seconds

    for attempt in range(MAX_RETRIES):
        try:
            result = subprocess.run(full_command, shell=True, check=True, capture_output=True, text=True)
            if result.returncode != 0:
                log.error(f"Command failed: {full_command}")
                log.error(result.stderr)
                raise RuntimeError(f"Command failed: {full_command}")
            return result.stdout
        except subprocess.CalledProcessError as e:
            stderr = str(e.stderr)
            if AZURE_THROTTLING_ERROR in stderr or RESOURCE_COLLECTION_THROTTLING_ERROR in stderr:
                if attempt < MAX_RETRIES - 1:
                    log.warning(f"Azure throttling ongoing. Retrying in {delay} seconds...")
                    sleep(delay)
                    delay *= 2
                    continue
                raise RateLimitExceeded("Rate limit exceeded. Please wait a few minutes and try again.") from e
            if REFRESH_TOKEN_EXPIRED_ERROR in stderr:
                raise RefreshTokenError(f"Auth token is expired. Refresh token before running '{az_cmd}'") from e
            if AUTHORIZATION_ERROR in stderr:
                try_regex_access_error(stderr)
                raise AuthError(f"Insufficient permissions to access resource when executing '{az_cmd}'") from e
            log.error(f"Command failed: {full_command}")
            log.error(e.stderr)
            raise RuntimeError(f"Command failed: {full_command}") from e

    raise SystemExit(1)  # unreachable


# =============================================================================
# VALIDATION
# =============================================================================
def validate_user_parameters(config: Configuration):
    validate_azure_values(config)
    validate_datadog_credentials(config.datadog_api_key, config.datadog_site)

    log.info("Validation completed")


def validate_azure_values(config: Configuration):
    """Validate Azure parameters and permissions before creating any resources."""

    validate_azure_configuration(config)
    validate_az_cli()
    validate_az_cli_extensions()
    validate_monitored_subscriptions(config.monitored_subscriptions)
    validate_control_plane_sub_access(config.control_plane_sub_id)
    validate_required_resource_providers(config.all_subscriptions)
    validate_resource_names(
        config.control_plane_rg, config.control_plane_sub_id, config.control_plane_cache_storage_name
    )


def validate_az_cli():
    """Ensure Azure CLI is installed and user is authenticated."""
    try:
        execute(AzCmd("account", "show"))
        log.debug("Azure CLI authentication verified")
    except Exception as e:
        raise RuntimeError("Azure CLI not authenticated. Run 'az login' first.") from e


def validate_az_cli_extensions():
    """Ensure required Azure CLI extensions are installed."""
    required_extension = "containerapp"

    try:
        output = execute(AzCmd("extension", "list").param("--output", "json"))
        installed_extensions = json.loads(output)
        installed_names = {ext["name"] for ext in installed_extensions}

        if required_extension not in installed_names:
            log.info(f"Installing missing Azure CLI extension: {required_extension}")
            execute(AzCmd("extension", "add").param("--name", required_extension))

        log.debug("Azure CLI extensions verified")
    except Exception as e:
        raise RuntimeError(f"Failed to validate/install Azure CLI extensions: {e}") from e


def validate_required_resource_providers(sub_ids: set[str]):
    """Ensure the required Azure resource providers are registered across all subscriptions."""
    required_providers = [
        "Microsoft.Web",  # Function Apps
        "Microsoft.App",  # Container Apps
        "Microsoft.Storage",  # Storage Accounts
        "Microsoft.Authorization",  # Role Assignments
    ]

    log.info(f"Checking required resource providers across {len(sub_ids)} subscription(s)...")

    sub_to_unregistered_provider_list = {}

    for sub_id in sub_ids:
        try:
            log.debug(f"Checking resource providers in subscription: {sub_id}")

            # Get all resource providers and their registration state
            output = execute(
                AzCmd("provider", "list")
                .param("--subscription", sub_id)
                .param("--query", '"[].{namespace:namespace, registrationState:registrationState}"')
                .param("--output", "json")
            )
            providers_status = json.loads(output)

            # Create a lookup dict
            provider_states = {p["namespace"]: p["registrationState"] for p in providers_status}

            unregistered_providers = []
            for provider in required_providers:
                state = provider_states.get(provider, "NotFound")
                if state != "Registered":
                    unregistered_providers.append(provider)
                    log.debug(f"Subscription {sub_id}: Resource provider {provider} is {state}")

            sub_to_unregistered_provider_list[sub_id] = unregistered_providers

            if unregistered_providers:
                log.info(
                    f"Subscription {sub_id}: Detected unregistered resource providers: {', '.join(unregistered_providers)}"
                )
            else:
                log.debug(f"Subscription {sub_id}: All required resource providers are registered")
        except Exception as e:
            log.error(f"Failed to validate resource providers in subscription {sub_id}: {e}")
            raise RuntimeError(f"Resource provider validation failed for subscription {sub_id}: {e}") from e

    success = True
    for sub_id, unregistered_providers in sub_to_unregistered_provider_list.items():
        if unregistered_providers:
            success = False
            log.error(
                f"Subscription {sub_id}: Detected unregistered resource providers: {', '.join(unregistered_providers)}"
            )
            log.error("Please run the following commands to register the missing resource providers:")
            log.error(f"az account set --subscription {sub_id}")
            for provider in unregistered_providers:
                log.error(f"az provider register --namespace {provider}")
        else:
            log.debug(f"Subscription {sub_id}: All required resource providers are registered")

    if not success:
        raise RuntimeError("Resource provider validation failed")

    log.info("Resource provider validation successful across all subscriptions")


def validate_control_plane_sub_access(control_plane_sub_id: str):
    """Verify access to the control plane subscription."""
    try:
        set_subscription(control_plane_sub_id)
        log.debug(f"Control plane subscription access verified: {control_plane_sub_id}")
    except Exception as e:
        raise RuntimeError(f"Cannot access control plane subscription {control_plane_sub_id}: {e}") from e


def validate_resource_names(control_plane_rg: str, control_plane_sub_id: str, control_plane_cache_storage_name: str):
    """Check if resource names are available and valid."""
    log.info("Validating resource name availability...")

    # Check if resource group already exists
    try:
        output = execute(
            AzCmd("group", "exists").param("--name", control_plane_rg).param("--subscription", control_plane_sub_id)
        )
        if output.strip().lower() == "true":
            log.warning(f"Resource group {control_plane_rg} already exists - will use existing")
        else:
            log.debug(f"Resource group name available: {control_plane_rg}")
    except Exception as e:
        raise RuntimeError(f"Cannot check resource group availability: {e}") from e

    # Check storage account name availability
    try:
        result_json = execute(AzCmd("storage", "account check-name").param("--name", control_plane_cache_storage_name))
        result = json.loads(result_json)
        if not result.get("nameAvailable", False):
            log.info(f"Storage account name '{control_plane_cache_storage_name}' exists - will use existing")
        log.debug(f"Storage account name available: {control_plane_cache_storage_name}")
    except json.JSONDecodeError as e:
        raise RuntimeError("Failed to parse storage account name availability check") from e


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
            f"DD-API-KEY:{datadog_api_key}",
        ]
        response = subprocess.check_output(curl_command, text=True)
        response_json = json.loads(response)
        if not response_json.get("valid", False):
            raise RuntimeError(f"Datadog API Key validation failed against {datadog_site}")

        log.debug("Datadog API credentials validated")
    except subprocess.CalledProcessError as e:
        raise ValueError(f"Failed to validate Datadog credentials: {e}") from e
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse Datadog validation response: {e}") from e


def validate_azure_configuration(config: Configuration):
    """Validate Azure configuration parameters."""
    log.info("Validating Azure configuration parameters...")

    if not config.control_plane_sub_id:
        raise ValueError("Control plane subscription not configured")

    if not config.control_plane_rg:
        raise ValueError("Control plane resource group not configured")

    if not config.control_plane_region:
        raise ValueError("Control plane location not configured")

    if not config.monitored_subscriptions:
        raise ValueError("Monitored subscriptions not properly configured")

    if config.log_level not in ["DEBUG", "INFO", "WARNING", "ERROR"]:
        raise ValueError(f"Invalid log level: {config.log_level}. Must be one of: DEBUG, INFO, WARNING, ERROR")

    log.debug("Configuration validation completed")


def validate_monitored_subscriptions(monitored_subs: list[str]):
    """Verify access to all monitored subscriptions."""
    log.info("Validating access to monitored subscriptions...")

    for sub_id in monitored_subs:
        try:
            set_subscription(sub_id)
            log.debug(f"Monitored subscription access verified: {sub_id}")
        except Exception as e:
            raise RuntimeError(f"Cannot access monitored subscription {sub_id}: {e}") from e


# =============================================================================
# RESOURCE SETUP - Subscription, Resource Group, Storage Account
# =============================================================================


def set_subscription(sub_id: str):
    log.debug(f"Setting active subscription to {sub_id}")
    execute(AzCmd("account", "set").param("--subscription", sub_id))


def create_resource_group(control_plane_rg: str, control_plane_region: str):
    """Create resource group for control plane"""
    log.info(f"Creating resource group {control_plane_rg} in {control_plane_region}")
    execute(AzCmd("group", "create").param("--name", control_plane_rg).param("--location", control_plane_region))


def create_storage_account(storage_account_name: str, control_plane_rg: str, control_plane_region: str):
    """Create storage account for control plane cache"""
    log.info(f"Creating storage account {storage_account_name}")
    execute(
        AzCmd("storage", "account create")
        .param("--name", storage_account_name)
        .param("--resource-group", control_plane_rg)
        .param("--location", control_plane_region)
        .param("--sku", "Standard_LRS")
        .param("--kind", "StorageV2")
        .param("--access-tier", "Hot")
        .param("--min-tls-version", "TLS1_2")
        .flag("--https-only")
    )


def create_blob_container(storage_account_name: str, control_plane_cache: str, account_key: str):
    """Create blob container for control plane cache"""
    log.info(f"Creating blob container {control_plane_cache}")
    execute(
        AzCmd("storage", "container create")
        .param("--account-name", storage_account_name)
        .param("--account-key", account_key)
        .param("--name", control_plane_cache)
    )


def create_file_share(storage_account_name: str, control_plane_cache: str, control_plane_rg: str):
    """Create file share for control plane cache"""
    log.info(f"Creating file share {control_plane_cache}")
    execute(
        AzCmd("storage", "share-rm create")
        .param("--storage-account", storage_account_name)
        .param("--name", control_plane_cache)
        .param("--resource-group", control_plane_rg)
    )


# =============================================================================
# RESOURCE SETUP - App Service Plan, Function Apps
# =============================================================================


def create_app_service_plan(app_service_plan: str, control_plane_rg: str, control_plane_region: str):
    """Create app service plan that the function apps slot into"""
    try:
        log.info(f"Checking if App Service Plan '{app_service_plan}' already exists...")
        execute(
            AzCmd("appservice", "plan show")
            .param("--name", app_service_plan)
            .param("--resource-group", control_plane_rg)
        )
        log.info(f"App Service Plan '{app_service_plan}' already exists - reusing existing plan")
        return
    except RuntimeError:
        log.info(f"App Service Plan '{app_service_plan}' not found - creating new plan")
        pass

    log.info(f"Creating App Service Plan {app_service_plan}")

    # Use `az resource create` instead of `az appservice plan create` because of Azure CLI issue with the SKU we utilize (Y1)
    # https://github.com/Azure/azure-cli/issues/19864
    execute(
        AzCmd("resource", "create")
        .param("--resource-group", control_plane_rg)
        .param("--name", app_service_plan)
        .param("--resource-type", "Microsoft.Web/serverfarms")
        .flag("--is-full-object")
        .param(
            "--properties",
            json.dumps(
                {
                    "name": app_service_plan,
                    "location": control_plane_region,
                    "kind": "linux",
                    "sku": {"name": "Y1", "tier": "Dynamic"},
                    "properties": {"reserved": True},
                }
            ),
        )
        .param("--api-version", "2022-09-01")
    )


def create_function_app(config: Configuration, name: str):
    """Create function app and configure settings depending on task type"""
    try:
        log.info(f"Checking if Function App '{name}' already exists...")
        execute(AzCmd("functionapp", "show").param("--name", name).param("--resource-group", config.control_plane_rg))
        log.info(f"Function App '{name}' already exists - skipping creation and updating configuration")
        function_app_exists = True
    except RuntimeError:
        log.info(f"Function App '{name}' not found - creating new function app")
        function_app_exists = False

    if not function_app_exists:
        log.info(f"Creating Function App {name}")
        execute(
            AzCmd("functionapp", "create")
            .param("--resource-group", config.control_plane_rg)
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
            f"RESOURCE_GROUP={config.control_plane_rg}",
        ]
    elif "scaling" in name:
        specific_settings = [
            f"RESOURCE_GROUP={config.control_plane_rg}",
            f"FORWARDER_IMAGE={config.image_registry}/forwarder:latest",
            f"CONTROL_PLANE_REGION={config.control_plane_region}",
            f"PII_SCRUBBER_RULES={config.pii_scrubber_rules}",
        ]
    else:
        specific_settings = []

    all_settings = common_settings + specific_settings

    log.debug(f"Configuring app settings for Function App {name}")
    execute(
        AzCmd("functionapp", "config appsettings set")
        .param("--name", name)
        .param("--resource-group", config.control_plane_rg)
        .param_list("--settings", all_settings)
    )

    log.debug(f"Configuring Linux runtime for Function App {name}")
    execute(
        AzCmd("functionapp", "config set")
        .param("--name", name)
        .param("--resource-group", config.control_plane_rg)
        .param("--linux-fx-version", '"Python|3.11"')
    )


def create_function_apps(config: Configuration):
    """Create Resources Task, Scaling Task, and Diagnostic Settings Task function apps"""
    log.info("Creating App Service Plan...")
    create_app_service_plan(config.app_service_plan, config.control_plane_rg, config.control_plane_region)

    log.info("Creating Function Apps...")
    for _, app_name in config.control_plane_function_apps.items():
        log.info(f"Creating Function App: {app_name}")
        create_function_app(config, app_name)

    log.info("Function Apps created and configured")


# =============================================================================
# RESOURCE SETUP - Container App Environment, Deployer Job, Custom ContainerAppStart Role Definition
# =============================================================================


def create_user_assigned_identity(control_plane_rg: str, control_plane_region: str):
    """Create a user-assigned managed identity if it does not exist"""
    identity_name = "runInitialDeployIdentity"

    try:
        log.info("Checking if user-assigned managed identity already exists...")
        execute(AzCmd("identity", "show").param("--name", identity_name).param("--resource-group", control_plane_rg))
        log.info(f"User-assigned managed identity '{identity_name}' already exists - reusing existing identity")
        return
    except RuntimeError:
        log.info("User-assigned managed identity not found - creating new identity")
        pass

    execute(
        AzCmd("identity", "create")
        .param("--name", identity_name)
        .param("--resource-group", control_plane_rg)
        .param("--location", control_plane_region)
    )


def create_containerapp_environment(
    control_plane_env: str, control_plane_resource_group: str, control_plane_location: str
):
    """Create the Container App environment if it does not exist"""

    try:
        log.info(f"Checking if Container App environment '{control_plane_env}' already exists...")
        execute(
            AzCmd("containerapp", "env show")
            .param("--name", control_plane_env)
            .param("--resource-group", control_plane_resource_group)
        )
        log.info(f"Container App environment '{control_plane_env}' already exists - reusing existing environment")
        return
    except RuntimeError:
        log.info(f"Container App environment '{control_plane_env}' not found - creating new environment")
        pass

    log.info(f"Creating Container App environment {control_plane_env}")
    execute(
        AzCmd("containerapp", "env create")
        .param("--name", control_plane_env)
        .param("--resource-group", control_plane_resource_group)
        .param("--location", control_plane_location)
    )


def create_containerapp_job(config: Configuration):
    """Create the Container App job for the deployer if it does not exist"""

    try:
        log.info(f"Checking if Container App job '{config.deployer_job_name}' already exists...")
        execute(
            AzCmd("containerapp", "job show")
            .param("--name", config.deployer_job_name)
            .param("--resource-group", config.control_plane_rg)
        )
        log.info(f"Container App job '{config.deployer_job_name}' already exists - reusing existing job")
        return
    except RuntimeError:
        log.info(f"Container App job '{config.deployer_job_name}' not found - creating new job")
        pass

    log.info(f"Creating Container App job {config.deployer_job_name}")

    env_vars = [
        "AzureWebJobsStorage=secretref:connection-string",
        f"SUBSCRIPTION_ID={config.control_plane_sub_id}",
        f"RESOURCE_GROUP={config.control_plane_rg}",
        f"CONTROL_PLANE_ID={config.control_plane_id}",
        f"CONTROL_PLANE_REGION={config.control_plane_region}",
        "DD_API_KEY=secretref:dd-api-key",
        f"DD_SITE={config.datadog_site}",
        f"DD_TELEMETRY={'true' if config.datadog_telemetry else 'false'}",
        f"STORAGE_ACCOUNT_URL={config.lfo_public_storage_account_url}",
        f"LOG_LEVEL={config.log_level}",
    ]

    secrets = [
        f"connection-string={config.get_control_plane_cache_conn_string()}",
        f"dd-api-key={config.datadog_api_key}",
    ]

    execute(
        AzCmd("containerapp", "job create")
        .param("--name", config.deployer_job_name)
        .param("--resource-group", config.control_plane_rg)
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
    """Create a custom role for starting container app jobs if it does not exist"""

    scope = execute(
        AzCmd("group", "show")
        .param("--name", control_plane_resource_group)
        .param("--query", "id")
        .param("--output", "tsv")
    ).strip()

    try:
        log.info(f"Checking if custom role definition '{container_app_start_role}' already exists...")
        output = execute(
            AzCmd("role", "definition list")
            .param("--name", container_app_start_role)
            .param("--scope", scope)
            .param("--output", "tsv")
        )
        if output.strip():
            log.info(f"Custom role definition '{container_app_start_role}' already exists - reusing existing role")
            return
        log.info(f"Custom role definition '{container_app_start_role}' not found - creating new role")
    except RuntimeError:
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

    execute(AzCmd("role", "definition create").param("--role-definition", "custom_role.json"))


def assign_custom_role_to_identity(control_plane_resource_group: str, container_app_start_role: str):
    """Assign the custom role to the managed identity if the role assignment does not exist"""
    log.info("Assigning custom role to managed identity")
    identity_id = execute(
        AzCmd("identity", "show")
        .param("--name", "runInitialDeployIdentity")
        .param("--resource-group", control_plane_resource_group)
        .param("--query", "principalId")
        .param("--output", "tsv")
    ).strip()

    scope = execute(
        AzCmd("group", "show")
        .param("--name", control_plane_resource_group)
        .param("--query", "id")
        .param("--output", "tsv")
    ).strip()

    role_id = execute(
        AzCmd("role", "definition list")
        .param("--name", container_app_start_role)
        .param("--scope", scope)
        .param("--query", '"[0].name"')
        .param("--output", "tsv")
    ).strip()

    try:
        log.debug(
            f"Checking if custom role assignment already exists for role {container_app_start_role} to identity {identity_id}"
        )
        output = execute(
            AzCmd("role", "assignment list")
            .param("--assignee", identity_id)
            .param("--role", role_id)
            .param("--scope", scope)
            .param("--query", '"length([])"')
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
        log.debug("Custom role assignment not found - creating new assignment")
        pass

    execute(
        AzCmd("role", "assignment create")
        .param("--role", role_id)
        .param("--assignee-object-id", identity_id)
        .param("--assignee-principal-type", "ServicePrincipal")
        .param("--scope", scope)
    )


def deploy_container_job_infra(config: Configuration):
    """Deploy all container job infrastructure."""
    log.info("Creating managed identity...")
    create_user_assigned_identity(config.control_plane_rg, config.control_plane_region)

    log.info("Creating container app environment...")
    create_containerapp_environment(config.control_plane_env, config.control_plane_rg, config.control_plane_region)

    log.info("Creating container app job...")
    create_containerapp_job(config)

    log.info("Defining custom ContainerAppStart role...")
    create_custom_role_definition(config.container_app_start_role, config.control_plane_rg)

    log.info("Assigning custom role to identity...")
    assign_custom_role_to_identity(config.control_plane_rg, config.container_app_start_role)

    log.info("Container App job + identity setup complete")


# =============================================================================
# RBAC PERMISSIONS ACROSS SUBSCRIPTIONS
# =============================================================================


def get_function_app_principal_id(control_plane_resource_group: str, function_app_name: str) -> str:
    """Get the principal ID of a Function App's managed identity."""
    log.debug(f"Getting principal ID for Function App {function_app_name}")
    output = execute(
        AzCmd("functionapp", "identity show")
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
        AzCmd("containerapp", "job show")
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
            AzCmd("role", "assignment list")
            .param("--assignee", principal_id)
            .param("--role", role_id)
            .param("--scope", scope)
            .param("--query", '"length([])"')
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
        AzCmd("role", "assignment create")
        .param("--assignee-object-id", principal_id)
        .param("--assignee-principal-type", "ServicePrincipal")
        .param("--role", role_id)
        .param("--scope", scope)
        .param("--description", f"ddlfo{control_plane_id}")
    )


def grant_permissions(config: Configuration):
    """Grant permissions across all monitored subscriptions."""
    log.info("Setting up permissions across monitored subscriptions...")

    MONITORING_READER_ID = "43d0d8ad-25c7-4714-9337-8ba259a9fe05"
    MONITORING_CONTRIBUTOR_ID = "749f88d5-cbae-40b8-bcfc-e573ddc772fa"
    STORAGE_READER_AND_DATA_ACCESS_ID = "c12c1c16-33a1-487b-954d-41c89c60f349"
    SCALING_CONTRIBUTOR_ID = "b24988ac-6180-42a0-ab88-20f7382dd24c"
    WEBSITE_CONTRIBUTOR_ID = "de139f84-1756-47ae-9be6-808fbbe84772"

    log.info("Assigning Website Contributor role to deployer container app job...")
    deployer_principal_id = get_containerapp_job_principal_id(config.control_plane_rg, config.deployer_job_name)
    assign_role(
        config.control_plane_resource_group_id,
        deployer_principal_id,
        WEBSITE_CONTRIBUTOR_ID,
        config.control_plane_id,
    )

    resource_task_pid = get_function_app_principal_id(
        config.control_plane_rg, config.control_plane_function_apps["resources"]
    )
    diagnostic_pid = get_function_app_principal_id(
        config.control_plane_rg, config.control_plane_function_apps["diagnostic"]
    )
    scaling_pid = get_function_app_principal_id(config.control_plane_rg, config.control_plane_function_apps["scaling"])

    for sub_id in config.monitored_subscriptions:
        log.info(f"Assigning permissions in subscription: {sub_id}")
        set_subscription(sub_id)
        execute(
            AzCmd("group", "create")
            .param("--name", config.control_plane_rg)
            .param("--location", config.control_plane_region)
        )

        subscription_scope = f"/subscriptions/{sub_id}"
        resource_group_scope = f"{subscription_scope}/resourceGroups/{config.control_plane_rg}"

        assign_role(subscription_scope, resource_task_pid, MONITORING_READER_ID, config.control_plane_id)
        assign_role(subscription_scope, diagnostic_pid, MONITORING_CONTRIBUTOR_ID, config.control_plane_id)
        assign_role(resource_group_scope, diagnostic_pid, STORAGE_READER_AND_DATA_ACCESS_ID, config.control_plane_id)
        assign_role(resource_group_scope, scaling_pid, SCALING_CONTRIBUTOR_ID, config.control_plane_id)

    set_subscription(config.control_plane_sub_id)
    log.info("Subscription permission setup complete")


# =============================================================================
# CONTROL PLANE DEPLOYMENT
# =============================================================================


def deploy_control_plane(config: Configuration):
    """Deploy all control plane infrastructure: storage, functions, and containers."""
    log.info("Deploying storage account...")
    set_subscription(config.control_plane_sub_id)
    create_storage_account(
        config.control_plane_cache_storage_name, config.control_plane_rg, config.control_plane_region
    )
    log.info("Waiting for storage account to be ready...")
    time.sleep(10)  # Ensure the storage account is ready
    key = get_storage_acct_key(config.control_plane_cache_storage_name, config.control_plane_rg)
    create_blob_container(config.control_plane_cache_storage_name, config.control_plane_cache, key)
    create_file_share(config.control_plane_cache_storage_name, config.control_plane_cache, config.control_plane_rg)
    log.info("Storage account setup completed")

    log.info("Creating Function Apps...")
    create_function_apps(config)

    log.info("Deploying Container App infrastructure...")
    deploy_container_job_infra(config)

    log.info("Control plane infrastructure deployment completed")


# =============================================================================
# INITIAL DEPLOYMENT TRIGGER
# =============================================================================


def run_initial_deploy(deployer_job_name: str, control_plane_rg: str, control_plane_sub_id: str):
    """Trigger the initial deployment by starting the deployer container app job."""
    log.info("Triggering initial deployment by starting deployer container app job...")

    try:
        execute(
            AzCmd("containerapp", "job start")
            .param("--name", deployer_job_name)
            .param("--resource-group", control_plane_rg)
            .param("--subscription", control_plane_sub_id)
            .flag("--no-wait")
        )

        log.info(f"Successfully started deployer job: {deployer_job_name}")
    except Exception as e:
        log.error(f"Failed to start deployer container app job: {e}")
        raise RuntimeError(f"Could not trigger initial deployment: {e}") from e


# =============================================================================
# MAIN INSTALLATION FLOW
# =============================================================================


def log_header(message: str):
    log.info("=" * 70)
    log.info(message)
    log.info("=" * 70)


def main():
    """Main installation flow that orchestrates all steps."""

    try:
        args = parse_arguments()
        config = Configuration(
            management_group_id=args.management_group,
            control_plane_region=args.control_plane_region,
            control_plane_sub_id=args.control_plane_subscription,
            control_plane_rg=args.control_plane_resource_group,
            monitored_subs=args.monitored_subscriptions,
            datadog_api_key=args.datadog_api_key,
            datadog_site=args.datadog_site,
            resource_tag_filters_arg=args.resource_tag_filters,
            pii_scrubber_rules_arg=args.pii_scrubber_rules,
            datadog_telemetry_arg=args.datadog_telemetry,
            log_level_arg=args.log_level,
        )

        # Set up logging based on config
        basicConfig(level=getattr(__import__("logging"), config.log_level))

        log.info("Starting setup for Azure Automated Log Forwarding...")

        log_header("STEP 1: Validating user configuration...")
        validate_user_parameters(config)

        set_subscription(config.control_plane_sub_id)

        log_header("STEP 2: Creating control plane resource group...")
        set_subscription(config.control_plane_sub_id)
        create_resource_group(config.control_plane_rg, config.control_plane_region)
        log.info("Control plane resource group created")

        log_header("STEP 3: Deploying control plane infrastructure...")
        deploy_control_plane(config)

        log_header("STEP 4: Setting up subscription permissions...")
        grant_permissions(config)
        log.info("Subscription and resource group permissions configured")

        log_header("STEP 5: Triggering initial deploy...")
        run_initial_deploy(config.deployer_job_name, config.control_plane_rg, config.control_plane_sub_id)
        log.info("Initial deployment triggered")

        log_header("Success! Azure Automated Log Forwarding installation completed!")

    except Exception as e:
        log.error(f"Failed with error: {e}")
        log.error("Check the Azure CLI output above for more details")
        raise


if __name__ == "__main__":
    main()
