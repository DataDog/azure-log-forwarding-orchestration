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
import shlex
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from logging import WARNING, basicConfig, getLogger
from re import search
from time import sleep

getLogger("azure").setLevel(WARNING)
log = getLogger("installer")

# =============================================================================
# CONSTANTS
# =============================================================================

IMAGE_REGISTRY_URL = "datadoghq.azurecr.io"
LFO_PUBLIC_STORAGE_ACCOUNT_URL = "https://ddazurelfo.blob.core.windows.net"
CONTROL_PLANE_CACHE = "control-plane-cache"
INITIAL_DEPLOY_IDENTITY_NAME = "runInitialDeployIdentity"
STORAGE_ACCOUNT_KEY_FULL_PERMISSIONS = "FULL"
REQUIRED_RESOURCE_PROVIDERS = [
    "Microsoft.Web",  # Function Apps
    "Microsoft.App",  # Container Apps
    "Microsoft.Storage",  # Storage Accounts
    "Microsoft.Authorization",  # Role Assignments
]
MONITORING_READER_ID = "43d0d8ad-25c7-4714-9337-8ba259a9fe05"
MONITORING_CONTRIBUTOR_ID = "749f88d5-cbae-40b8-bcfc-e573ddc772fa"
STORAGE_READER_AND_DATA_ACCESS_ID = "c12c1c16-33a1-487b-954d-41c89c60f349"
SCALING_CONTRIBUTOR_ID = "b24988ac-6180-42a0-ab88-20f7382dd24c"
WEBSITE_CONTRIBUTOR_ID = "de139f84-1756-47ae-9be6-808fbbe84772"


# =============================================================================
# ERRORS
# =============================================================================
# Errors that prevent script from completing successfully
class FatalError(Exception):
    """An error that prevents the installation from completing successfully."""

    pass


class TimeoutError(FatalError):
    """An error that indicates a timeout occurred when waiting for a resource to be ready."""

    pass


class ExistenceCheckError(FatalError):
    """An error that occurs when checking if a resource exists."""

    pass


class RefreshTokenError(FatalError):
    """An error that indicates our auth token has expired."""

    pass


# Errors users can resolve through manual action
class UserActionRequiredError(Exception):
    """An error that requires user action to resolve."""

    pass


class AccessError(UserActionRequiredError):
    """An error that indicates we are not authorized to access the resource."""

    pass


class InputParamValidationError(UserActionRequiredError):
    """An error that indicates a validation error in user input parameters."""

    pass


class ResourceProviderRegistrationValidationError(UserActionRequiredError):
    """An error that indicates a resource provider is not registered."""

    pass


class ResourceNameAvailabilityError(UserActionRequiredError):
    """An error that indicates a resource name is not available."""

    pass


class DatadogAccessValidationError(UserActionRequiredError):
    """An error that indicates we are not authorized to access Datadog."""

    pass


# Expected Errors
class RateLimitExceededError(Exception):
    """An error that indicates we have exceeded the rate limit for the Azure API. Script will retry until MAX_RETRIES are reached."""

    pass


class ResourceNotFoundError(Exception):
    """An error that indicates a resource was not found. This gets thrown during some resource existence checks."""

    pass


# =============================================================================
# CONFIGURATION INPUT PARAMETERS
# =============================================================================


@dataclass
class Configuration:
    """User-specified configuration parameters and derivations necessary for deployment"""

    # Required user-specified params
    management_group_id: str
    control_plane_region: str
    control_plane_sub_id: str
    control_plane_rg: str
    monitored_subs: str
    datadog_api_key: str

    # Optional user-specified params with defaults
    datadog_site: str = "datadoghq.com"
    resource_tag_filters: str = ""
    pii_scrubber_rules: str = ""
    datadog_telemetry: bool = False
    log_level: str = "INFO"

    # Constants
    NIL_UUID = "00000000-0000-0000-0000-000000000000"

    def generate_control_plane_id(self) -> str:
        """Returns a 12-character unique ID based on user input parameters.
        This ID is suffixed on Azure resources we create to identify their relationship to the control plane.
        """

        combined = (
            f"{self.management_group_id}{self.control_plane_sub_id}{self.control_plane_rg}{self.control_plane_region}"
        )

        namespace = uuid.UUID(self.NIL_UUID)
        guid = str(uuid.uuid5(namespace, combined)).lower()
        return guid[:8] + guid[9:13]

    def get_control_plane_cache_key(self) -> str:
        if self.control_plane_cache_storage_key:
            return self.control_plane_cache_storage_key

        log.debug(f"Retrieving storage account key for {self.control_plane_cache_storage_name}")

        try:
            output = execute(
                AzCmd("storage", "account keys list")
                .param("--account-name", self.control_plane_cache_storage_name)
                .param("--resource-group", self.control_plane_rg)
            )
            keys_json = json.loads(output)

            if not isinstance(keys_json, list) or len(keys_json) == 0:
                raise FatalError(f"Failed to retrieve storage account keys for {self.control_plane_cache_storage_name}")

            for key_entry in keys_json:
                if key_entry.get("permissions") == STORAGE_ACCOUNT_KEY_FULL_PERMISSIONS and key_entry.get("value"):
                    self.control_plane_cache_storage_key = key_entry["value"]
                    break
            else:
                raise FatalError(
                    f"No storage account keys with full read/write permissions found for {self.control_plane_cache_storage_name}"
                )
        except json.JSONDecodeError as e:
            raise FatalError(
                f"Failed to parse storage account keys for {self.control_plane_cache_storage_name}: {e}"
            ) from e
        except KeyError as e:
            raise FatalError(
                f"Failed to retrieve storage account keys for {self.control_plane_cache_storage_name}: {e}"
            ) from e

        return self.control_plane_cache_storage_key

    def get_control_plane_cache_conn_string(self) -> str:
        return f"DefaultEndpointsProtocol=https;AccountName={self.control_plane_cache_storage_name};EndpointSuffix=core.windows.net;AccountKey={self.get_control_plane_cache_key()}"

    def __post_init__(self):
        """Calculates derived values from user-specified params."""

        self.monitored_subscriptions = [sub.strip() for sub in self.monitored_subs.split(",") if sub.strip()]
        self.all_subscriptions = {
            self.control_plane_sub_id,
            *self.monitored_subscriptions,
        }

        # Control plane
        self.control_plane_id = self.generate_control_plane_id()
        log.info(f"Generated control plane ID: {self.control_plane_id}")
        self.control_plane_cache_storage_name = f"lfostorage{self.control_plane_id}"
        self.control_plane_cache_storage_url = f"https://{self.control_plane_cache_storage_name}.blob.core.windows.net"
        self.control_plane_cache_storage_key = None  # lazy-loaded
        self.control_plane_sub_scope = f"/subscriptions/{self.control_plane_sub_id}"
        self.control_plane_rg_scope = f"{self.control_plane_sub_scope}/resourceGroups/{self.control_plane_rg}"
        self.control_plane_env_name = f"dd-log-forwarder-env-{self.control_plane_id}-{self.control_plane_region}"

        # Deployer
        self.deployer_job_name = f"deployer-task-{self.control_plane_id}"
        self.deployer_image_url = f"{IMAGE_REGISTRY_URL}/deployer:latest"
        self.container_app_start_role_name = f"ContainerAppStartRole{self.control_plane_id}"

        # Function apps (control plane tasks)
        self.app_service_plan_name = f"control-plane-asp-{self.control_plane_id}"
        self.resources_task_name = f"resources-task-{self.control_plane_id}"
        self.scaling_task_name = f"scaling-task-{self.control_plane_id}"
        self.diagnostic_settings_task_name = f"diagnostic-settings-task-{self.control_plane_id}"
        self.control_plane_function_app_names = [
            self.resources_task_name,
            self.scaling_task_name,
            self.diagnostic_settings_task_name,
        ]


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Azure Log Forwarding Orchestration Installation Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Required parameters
    parser.add_argument(
        "-mg",
        "--management-group",
        type=str,
        required=True,
        help="Management group ID to deploy under (required)",
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
        "--resource-tag-filters",
        type=str,
        default="",
        help="Comma separated list of tags to filter resources by",
    )

    parser.add_argument(
        "--pii-scrubber-rules",
        type=str,
        default="",
        help="YAML formatted list of PII Scrubber Rules",
    )

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
RESOURCE_NOT_FOUND_ERROR = "ResourceNotFound"
MAX_RETRIES = 7


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

    def str(self) -> str:
        return "az " + " ".join(self.cmd)


def check_access_error(stderr: str) -> str | None:
    # Sample:
    # (AuthorizationFailed) The client 'user@example.com' with object id '00000000-0000-0000-0000-000000000000'
    # does not have authorization to perform action 'Microsoft.Storage/storageAccounts/read'
    # over scope '/subscriptions/00000000-0000-0000-0000-000000000000' or the scope is invalid.
    # If access was recently granted, please refresh your credentials.

    client_match = search(r"client '([^']*)'", stderr)
    action_match = search(r"action '([^']*)'", stderr)
    scope_match = search(r"scope '([^']*)'", stderr)

    if not (action_match and scope_match and client_match):
        return

    client = client_match.group(1)
    action = action_match.group(1)
    scope = scope_match.group(1)
    return f"Insufficient permissions for {client} to perform {action} on {scope}"


def execute(az_cmd: AzCmd) -> str:
    """Run an Azure CLI command and return output or raise error."""

    full_command = az_cmd.str()
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
            if RESOURCE_NOT_FOUND_ERROR in stderr:
                raise ResourceNotFoundError(f"Resource not found when executing '{az_cmd}'") from e
            if AZURE_THROTTLING_ERROR in stderr or RESOURCE_COLLECTION_THROTTLING_ERROR in stderr:
                if attempt < MAX_RETRIES - 1:
                    log.warning(f"Azure throttling ongoing. Retrying in {delay} seconds...")
                    sleep(delay)
                    delay *= 2
                    continue
                raise RateLimitExceededError("Rate limit exceeded. Please wait a few minutes and try again.") from e
            if REFRESH_TOKEN_EXPIRED_ERROR in stderr:
                raise RefreshTokenError(f"Auth token is expired. Refresh token before running '{az_cmd}'") from e
            if AUTHORIZATION_ERROR in stderr:
                error_message = f"Insufficient permissions to access resource when executing '{az_cmd}'"
                error_details = check_access_error(stderr)
                if error_details:
                    raise AccessError(f"{error_message}: {error_details}") from e
                raise AccessError(error_message) from e
            log.error(f"Command failed: {full_command}")
            log.error(e.stderr)
            raise RuntimeError(f"Command failed: {full_command}") from e

    raise SystemExit(1)  # unreachable


# =============================================================================
# VALIDATION
# =============================================================================
def validate_user_parameters(config: Configuration):
    validate_azure_env(config)
    validate_datadog_credentials(config.datadog_api_key, config.datadog_site)

    log.info("Validation completed")


def validate_azure_env(config: Configuration):
    """Validate Azure parameters and environment before creating any resources."""

    validate_user_config(config)
    validate_az_cli()
    validate_control_plane_sub_access(config.control_plane_sub_id)
    validate_monitored_subs_access(config.monitored_subscriptions)
    validate_resource_provider_registrations(config.all_subscriptions)
    validate_resource_names(
        config.control_plane_rg,
        config.control_plane_sub_id,
        config.control_plane_cache_storage_name,
    )


def validate_az_cli():
    """Ensure Azure CLI is installed and user is authenticated."""
    try:
        execute(AzCmd("account", "show"))
        log.debug("Azure CLI authentication verified")
    except Exception as e:
        raise AccessError("Azure CLI not authenticated. Run 'az login' first.") from e


def check_providers_per_subscription(sub_ids: set[str]) -> dict[str, list[str]]:
    """Check resource providers per subscription and return a dict of subscription IDs to unregistered providers."""

    sub_to_unregistered_provider_list = {}

    for sub_id in sub_ids:
        try:
            log.debug(f"Checking resource providers in subscription: {sub_id}")

            # Get all resource providers and their registration state
            output = execute(
                AzCmd("provider", "list")
                .param("--subscription", sub_id)
                .param(
                    "--query",
                    '"[].{namespace:namespace, registrationState:registrationState}"',
                )
                .param("--output", "json")
            )
            providers_status = json.loads(output)

            # Create a lookup dict
            provider_states = {p["namespace"]: p["registrationState"] for p in providers_status}

            unregistered_providers = []
            for provider in REQUIRED_RESOURCE_PROVIDERS:
                state = provider_states.get(provider, "NotFound")
                if state != "Registered":
                    unregistered_providers.append(provider)
                    log.debug(f"Subscription {sub_id}: Resource provider {provider} is {state}")

            sub_to_unregistered_provider_list[sub_id] = unregistered_providers
        except Exception as e:
            log.error(f"Failed to validate resource providers in subscription {sub_id}: {e}")
            raise ResourceProviderRegistrationValidationError(
                f"Resource provider validation failed for subscription {sub_id}: {e}"
            ) from e

    return sub_to_unregistered_provider_list


def validate_resource_provider_registrations(sub_ids: set[str]):
    """Ensure the required Azure resource providers are registered across all subscriptions."""

    log.info(f"Checking required resource providers across {len(sub_ids)} subscription(s)...")
    sub_to_unregistered_provider_list = check_providers_per_subscription(sub_ids)

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
        raise ResourceProviderRegistrationValidationError(
            "Resource provider validation failed. Check logs for more details."
        )

    log.info("Resource provider validation successful across all subscriptions")


def validate_control_plane_sub_access(control_plane_sub_id: str):
    """Verify access to the control plane subscription."""
    try:
        set_subscription(control_plane_sub_id)
        log.debug(f"Control plane subscription access verified: {control_plane_sub_id}")
    except Exception as e:
        raise AccessError(f"Cannot access control plane subscription {control_plane_sub_id}: {e}") from e


def validate_resource_names(
    control_plane_rg: str,
    control_plane_sub_id: str,
    control_plane_cache_storage_name: str,
):
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
        raise ExistenceCheckError(f"Cannot check resource group availability: {e}") from e

    # Check storage account name availability
    try:
        result_json = execute(AzCmd("storage", "account check-name").param("--name", control_plane_cache_storage_name))
        result = json.loads(result_json)
        if not result.get("nameAvailable", False):
            log.info(f"Storage account name '{control_plane_cache_storage_name}' exists - will use existing")
        log.debug(f"Storage account name available: {control_plane_cache_storage_name}")
    except json.JSONDecodeError as e:
        raise ExistenceCheckError("Failed to parse storage account name availability check") from e


def validate_datadog_credentials(datadog_api_key: str, datadog_site: str):
    """Validate Datadog API credentials without making changes."""
    log.info("Validating Datadog API credentials...")

    if not datadog_api_key:
        raise InputParamValidationError("Datadog API key not configured")

    try:
        url = f"https://api.{datadog_site}/api/v1/validate"
        headers = {"Accept": "application/json", "DD-API-KEY": datadog_api_key}
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as response:
            response_json = json.loads(response.read())
            if not response_json.get("valid", False):
                raise DatadogAccessValidationError(f"Datadog API Key validation with {datadog_site} failed")

        log.debug("Datadog API credentials validated")
    except urllib.error.HTTPError as e:
        raise DatadogAccessValidationError(f"Failed to validate Datadog credentials: HTTP {e.code} {e.reason}") from e
    except urllib.error.URLError as e:
        raise DatadogAccessValidationError(f"Failed to validate Datadog credentials: {e.reason}") from e
    except json.JSONDecodeError as e:
        raise DatadogAccessValidationError(f"Failed to parse Datadog validation response: {e}") from e


def validate_user_config(config: Configuration):
    """Validate user-specified configuration parameters."""
    log.info("Validating configuration parameters...")

    if not config.control_plane_sub_id:
        raise InputParamValidationError("Control plane subscription not configured")

    if not config.control_plane_rg:
        raise InputParamValidationError("Control plane resource group not configured")

    if not config.control_plane_region:
        raise InputParamValidationError("Control plane location not configured")

    if not config.monitored_subscriptions:
        raise InputParamValidationError("Monitored subscriptions not properly configured.")

    if config.log_level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
        raise InputParamValidationError(
            f"Invalid log level: {config.log_level}. Must be one of: DEBUG, INFO, WARNING, ERROR"
        )

    log.debug("Configuration validation completed")


def validate_monitored_subs_access(monitored_subs: list[str]):
    """Verify access to all monitored subscriptions."""
    log.info("Validating access to monitored subscriptions...")

    for sub_id in monitored_subs:
        try:
            set_subscription(sub_id)
            log.debug(f"Monitored subscription access verified: {sub_id}")
        except Exception as e:
            raise AccessError(f"Cannot access monitored subscription {sub_id}: {e}") from e


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


def create_blob_container(storage_account_name: str, account_key: str):
    """Create blob container for control plane cache"""
    log.info(f"Creating blob container {CONTROL_PLANE_CACHE}")
    execute(
        AzCmd("storage", "container create")
        .param("--account-name", storage_account_name)
        .param("--account-key", account_key)
        .param("--name", CONTROL_PLANE_CACHE)
    )


def create_file_share(storage_account_name: str, control_plane_rg: str):
    """Create file share for control plane cache"""
    log.info(f"Creating file share {CONTROL_PLANE_CACHE}")
    execute(
        AzCmd("storage", "share-rm create")
        .param("--storage-account", storage_account_name)
        .param("--name", CONTROL_PLANE_CACHE)
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
    except ResourceNotFoundError:
        log.info(f"App Service Plan '{app_service_plan}' not found - creating new plan")
    except Exception as e:
        raise ExistenceCheckError(f"Failed to check if App Service Plan '{app_service_plan}' exists: {e}") from e

    log.info(f"Creating App Service Plan {app_service_plan}")

    # Use `az resource create` instead of `az appservice plan create` because of Azure CLI issue with the SKU we utilize (Y1)
    # https://github.com/Azure/azure-cli/issues/19864

    properties_json = json.dumps(
        {
            "name": app_service_plan,
            "location": control_plane_region,
            "kind": "linux",
            "sku": {"name": "Y1", "tier": "Dynamic"},
            "properties": {"reserved": True},
        }
    )

    execute(
        AzCmd("resource", "create")
        .param("--resource-group", control_plane_rg)
        .param("--name", app_service_plan)
        .param("--resource-type", "Microsoft.Web/serverfarms")
        .flag("--is-full-object")
        .param("--properties", shlex.quote(properties_json))
        .param("--api-version", "2022-09-01")
    )


def create_function_app(config: Configuration, name: str):
    """Create function app and configure settings depending on task type"""
    try:
        log.info(f"Checking if Function App '{name}' already exists...")
        execute(AzCmd("functionapp", "show").param("--name", name).param("--resource-group", config.control_plane_rg))
        log.info(f"Function App '{name}' already exists - skipping creation and updating configuration")
        function_app_exists = True
    except ResourceNotFoundError:
        log.info(f"Function App '{name}' not found - creating new function app")
        function_app_exists = False
    except Exception as e:
        raise ExistenceCheckError(f"Failed to check if Function App '{name}' exists: {e}") from e

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

    common_settings = {
        "AzureWebJobsStorage": config.get_control_plane_cache_conn_string(),
        "FUNCTIONS_EXTENSION_VERSION": "~4",
        "FUNCTIONS_WORKER_RUNTIME": "python",
        "WEBSITE_CONTENTAZUREFILECONNECTIONSTRING": config.get_control_plane_cache_conn_string(),
        "WEBSITE_CONTENTSHARE": name,
        "AzureWebJobsFeatureFlags": "EnableWorkerIndexing",
        "DD_API_KEY": config.datadog_api_key,
        "DD_SITE": config.datadog_site,
        "DD_TELEMETRY": "true" if config.datadog_telemetry else "false",
        "CONTROL_PLANE_ID": config.control_plane_id,
        "LOG_LEVEL": config.log_level,
    }

    # Task-specific settings
    match name:
        case config.resources_task_name:
            specific_settings = {
                "MONITORED_SUBSCRIPTIONS": ",".join(config.monitored_subscriptions),
                "RESOURCE_TAG_FILTERS": config.resource_tag_filters,
            }
        case config.diagnostic_settings_task_name:
            specific_settings = {
                "RESOURCE_GROUP": config.control_plane_rg,
            }
        case config.scaling_task_name:
            specific_settings = {
                "RESOURCE_GROUP": config.control_plane_rg,
                "FORWARDER_IMAGE": f"{IMAGE_REGISTRY_URL}/forwarder:latest",
                "CONTROL_PLANE_REGION": config.control_plane_region,
                "PII_SCRUBBER_RULES": config.pii_scrubber_rules,
            }
        case _:
            raise FatalError(f"Unknown function app task when configuring app settings: {name}")

    all_settings = {**common_settings, **specific_settings}

    with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as tmpfile:
        json.dump(all_settings, tmpfile)
        tmpfile.flush()

        log.debug(f"Configuring app settings for Function App {name}")
        execute(
            AzCmd("functionapp", "config appsettings set")
            .param("--name", name)
            .param("--resource-group", config.control_plane_rg)
            .param("--settings", f"@{tmpfile.name}")
        )

    log.debug(f"Configuring Linux runtime for Function App {name}")
    execute(
        AzCmd("functionapp", "config set")
        .param("--name", name)
        .param("--resource-group", config.control_plane_rg)
        .param("--linux-fx-version", shlex.quote("Python|3.11"))
    )


def create_function_apps(config: Configuration):
    """Create Resources Task, Scaling Task, and Diagnostic Settings Task function apps"""
    log.info("Creating App Service Plan...")
    create_app_service_plan(config.app_service_plan_name, config.control_plane_rg, config.control_plane_region)

    log.info("Creating Function Apps...")
    for function_app_name in config.control_plane_function_app_names:
        log.info(f"Creating Function App: {function_app_name}")
        create_function_app(config, function_app_name)

    log.info("Function Apps created and configured")


# =============================================================================
# RESOURCE SETUP - Container App Environment, Deployer Job, Custom ContainerAppStart Role Definition
# =============================================================================


def create_initial_deploy_identity(control_plane_rg: str, control_plane_region: str):
    """Create a managed identity for initial deploy if it does not exist"""
    try:
        log.info("Checking if user-assigned managed identity already exists...")
        execute(
            AzCmd("identity", "show")
            .param("--name", INITIAL_DEPLOY_IDENTITY_NAME)
            .param("--resource-group", control_plane_rg)
        )
        log.info(
            f"User-assigned managed identity '{INITIAL_DEPLOY_IDENTITY_NAME}' already exists - reusing existing identity"
        )
        return
    except ResourceNotFoundError:
        log.info("User-assigned managed identity not found - creating new identity")

    execute(
        AzCmd("identity", "create")
        .param("--name", INITIAL_DEPLOY_IDENTITY_NAME)
        .param("--resource-group", control_plane_rg)
        .param("--location", control_plane_region)
    )


def create_container_app_environment(
    control_plane_env: str,
    control_plane_resource_group: str,
    control_plane_location: str,
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
    except ResourceNotFoundError:
        log.info(f"Container App environment '{control_plane_env}' not found - creating new environment")

    log.info(f"Creating Container App environment {control_plane_env}")
    execute(
        AzCmd("containerapp", "env create")
        .param("--name", control_plane_env)
        .param("--resource-group", control_plane_resource_group)
        .param("--location", control_plane_location)
    )


def create_container_app_job(config: Configuration):
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
    except ResourceNotFoundError:
        log.info(f"Container App job '{config.deployer_job_name}' not found - creating new job")

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
        shlex.quote(f"STORAGE_ACCOUNT_URL={LFO_PUBLIC_STORAGE_ACCOUNT_URL}"),
        f"LOG_LEVEL={config.log_level}",
    ]

    secrets = [
        shlex.quote(f"connection-string={config.get_control_plane_cache_conn_string()}"),
        shlex.quote(f"dd-api-key={config.datadog_api_key}"),
    ]

    execute(
        AzCmd("containerapp", "job create")
        .param("--name", config.deployer_job_name)
        .param("--resource-group", config.control_plane_rg)
        .param("--environment", config.control_plane_env_name)
        .param("--replica-timeout", "1800")
        .param("--replica-retry-limit", "1")
        .param("--trigger-type", "Schedule")
        .param("--cron-expression", shlex.quote("*/30 * * * *"))
        .param("--image", config.deployer_image_url)
        .param("--cpu", "0.5")
        .param("--memory", "1Gi")
        .param("--parallelism", "1")
        .param("--replica-completion-count", "1")
        .flag("--mi-system-assigned")
        .param_list("--env-vars", env_vars)
        .param_list("--secrets", secrets)
    )


def create_custom_container_app_start_role(role_name: str, role_scope: str):
    """Create a custom role for starting container app jobs if it does not exist"""

    try:
        log.info(f"Checking if custom role definition '{role_name}' already exists...")
        output = execute(
            AzCmd("role", "definition list")
            .param("--name", role_name)
            .param("--scope", role_scope)
            .param("--output", "tsv")
        )
        if output.strip():
            log.info(f"Custom role definition '{role_name}' already exists - reusing existing role")
            return

        # `az role definition list` returns empty string if the role definition doesn't exist
        log.info(f"Custom role definition '{role_name}' not found - creating new role")
    except Exception as e:
        raise ExistenceCheckError(f"Failed to check if custom role definition '{role_name}' exists: {e}") from e

    log.info(f"Creating custom role definition {role_name}")

    role_definition = {
        "Name": role_name,
        "IsCustom": True,
        "Description": "Custom role to start container app jobs",
        "Actions": ["Microsoft.App/jobs/start/action"],
        "NotActions": [],
        "AssignableScopes": [role_scope],
    }

    with open("custom_role.json", "w") as f:
        json.dump(role_definition, f)

    try:
        execute(AzCmd("role", "definition create").param("--role-definition", "custom_role.json"))
    except Exception as e:
        raise RuntimeError(f"Failed to create custom role definition '{role_name}': {e}") from e


def role_exists(role_id: str, scope: str, principal_id: str) -> bool:
    """Check if a role assignment exists for a given role, scope, and principal"""

    try:
        output = execute(
            AzCmd("role", "assignment list")
            .param("--assignee", principal_id)
            .param("--role", role_id)
            .param("--scope", scope)
            .param("--query", '"length([])"')
            .param("--output", "tsv")
        )

        return int(output.strip()) > 0
    except (RuntimeError, ValueError) as e:
        log.error(f"Failed to check if role assignment exists: {e}")
        return False


def assign_custom_role_to_identity(
    role_name: str,
    role_id: str,
    control_plane_resource_group: str,
    control_plane_rg_scope: str,
):
    """Assign the custom role to the managed identity if the role assignment does not exist"""
    log.info("Assigning custom role to managed identity")
    identity_id = execute(
        AzCmd("identity", "show")
        .param("--name", INITIAL_DEPLOY_IDENTITY_NAME)
        .param("--resource-group", control_plane_resource_group)
        .param("--query", "principalId")
        .param("--output", "tsv")
    ).strip()

    log.debug(f"Checking if custom role assignment already exists for role {role_name} to identity {identity_id}")
    if role_exists(role_id, control_plane_rg_scope, identity_id):
        log.info(f"Custom role assignment already exists for role {role_name} to managed identity - skipping")
        return
    log.debug("Custom role assignment not found - creating new assignment")

    execute(
        AzCmd("role", "assignment create")
        .param("--role", role_id)
        .param("--assignee-object-id", identity_id)
        .param("--assignee-principal-type", "ServicePrincipal")
        .param("--scope", control_plane_rg_scope)
    )


def wait_for_role_definition_ready(role_name: str, role_scope: str) -> str:
    """Waits for custom role definition to be available in Azure.
    Role definitions are created asynchronously, so we need to wait for them to be available.
    Returns the role ID when the role definition is ready.
    """
    log.info(f"Waiting for role definition {role_name} to be ready...")

    start_time = time.time()
    max_wait_seconds = 120
    poll_interval = 5  # seconds
    while time.time() - start_time < max_wait_seconds:
        try:
            role_id = execute(
                AzCmd("role", "definition list")
                .param("--name", role_name)
                .param("--scope", role_scope)
                .param("--query", '"[0].name"')
                .param("--output", "tsv")
            ).strip()

            if role_id:
                log.info(f"Role definition {role_name} is ready")
                log.debug(f"ID: {role_id}")
                return role_id

        except RuntimeError as e:
            raise ExistenceCheckError(f"Failure to check if role definition {role_name} is ready: {e}") from e

        log.info(f"Role definition {role_name} not yet available, will check again in {poll_interval} seconds")
        time.sleep(poll_interval)

    raise TimeoutError(f"Timeout waiting for role definition {role_name} to be ready after {max_wait_seconds} seconds")


def deploy_initial_deploy_role(config: Configuration):
    log.info("Creating identity for initial deployment...")
    create_initial_deploy_identity(config.control_plane_rg, config.control_plane_region)

    log.info("Defining custom ContainerAppStart role...")
    role_scope = config.control_plane_rg_scope
    create_custom_container_app_start_role(config.container_app_start_role_name, role_scope)

    role_id = wait_for_role_definition_ready(config.container_app_start_role_name, role_scope)

    log.info("Assigning custom role to identity...")
    assign_custom_role_to_identity(
        config.container_app_start_role_name,
        role_id,
        config.control_plane_rg,
        role_scope,
    )


def deploy_lfo_deployer(config: Configuration):
    """Deploy all container job infrastructure."""
    deploy_initial_deploy_role(config)

    log.info("Creating container app environment...")
    create_container_app_environment(
        config.control_plane_env_name, config.control_plane_rg, config.control_plane_region
    )

    log.info("Creating container app job...")
    create_container_app_job(config)

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


def get_container_app_job_principal_id(control_plane_resource_group: str, job_name: str) -> str:
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

    log.debug(
        f"Checking if role assignment already exists for role {role_id} to principal {principal_id} at scope {scope}"
    )

    if role_exists(role_id, scope, principal_id):
        log.debug(
            f"Role assignment already exists for role {role_id} to principal {principal_id} at scope {scope} - skipping"
        )
        return
    log.debug("Role assignment not found - creating new assignment")

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

    log.info("Assigning Website Contributor role to deployer container app job...")
    deployer_principal_id = get_container_app_job_principal_id(config.control_plane_rg, config.deployer_job_name)
    assign_role(
        config.control_plane_rg_scope,
        deployer_principal_id,
        WEBSITE_CONTRIBUTOR_ID,
        config.control_plane_id,
    )

    resource_principal_id = get_function_app_principal_id(config.control_plane_rg, config.resources_task_name)
    scaling_principal_id = get_function_app_principal_id(config.control_plane_rg, config.scaling_task_name)
    diagnostic_principal_id = get_function_app_principal_id(
        config.control_plane_rg, config.diagnostic_settings_task_name
    )

    for sub_id in config.monitored_subscriptions:
        log.info(f"Create resource group in subscription: {sub_id}")
        set_subscription(sub_id)
        execute(
            AzCmd("group", "create")
            .param("--name", config.control_plane_rg)
            .param("--location", config.control_plane_region)
        )

        subscription_scope = f"/subscriptions/{sub_id}"
        resource_group_scope = f"{subscription_scope}/resourceGroups/{config.control_plane_rg}"

        log.info(f"Assigning permissions in subscription: {sub_id}")
        assign_role(
            subscription_scope,
            resource_principal_id,
            MONITORING_READER_ID,
            config.control_plane_id,
        )
        assign_role(
            resource_group_scope,
            scaling_principal_id,
            SCALING_CONTRIBUTOR_ID,
            config.control_plane_id,
        )
        assign_role(
            subscription_scope,
            diagnostic_principal_id,
            MONITORING_CONTRIBUTOR_ID,
            config.control_plane_id,
        )
        assign_role(
            resource_group_scope,
            diagnostic_principal_id,
            STORAGE_READER_AND_DATA_ACCESS_ID,
            config.control_plane_id,
        )

    set_subscription(config.control_plane_sub_id)
    log.info("Subscription permission setup complete")


# =============================================================================
# CONTROL PLANE DEPLOYMENT
# =============================================================================


def wait_for_storage_account_ready(storage_account_name: str, control_plane_rg: str) -> None:
    """Waits for storage account to be in 'Succeeded' provisioning state.
    Storage accounts are created asynchronously, so we need to wait for them to be ready.
    """
    log.info(f"Waiting for storage account {storage_account_name} to be ready...")

    start_time = time.time()
    max_wait_seconds = 60
    while time.time() - start_time < max_wait_seconds:
        output = execute(
            AzCmd("storage", "account show")
            .param("--name", storage_account_name)
            .param("--resource-group", control_plane_rg)
            .param("--query", "provisioningState")
            .param("--output", "tsv")
        )

        state = output.strip()
        log.debug(f"Storage account {storage_account_name} provisioning state: {state}")

        if state == "Succeeded":
            log.info(f"Storage account {storage_account_name} is ready")
            return
        elif state in ["Failed", "Canceled"]:
            raise RuntimeError(f"Storage account {storage_account_name} provisioning failed with state: {state}")

        # Still provisioning, wait and check again
        time.sleep(5)

    raise TimeoutError(
        f"Timeout waiting for storage account {storage_account_name} to be ready after {max_wait_seconds} seconds"
    )


def deploy_control_plane(config: Configuration):
    """Deploy all control plane infrastructure: storage, functions, and containers."""
    log.info("Deploying storage account...")
    set_subscription(config.control_plane_sub_id)
    create_storage_account(
        config.control_plane_cache_storage_name,
        config.control_plane_rg,
        config.control_plane_region,
    )
    wait_for_storage_account_ready(config.control_plane_cache_storage_name, config.control_plane_rg)
    create_blob_container(
        config.control_plane_cache_storage_name,
        config.get_control_plane_cache_key(),
    )
    create_file_share(
        config.control_plane_cache_storage_name,
        config.control_plane_rg,
    )
    log.info("Storage account setup completed")

    log.info("Creating Function Apps...")
    create_function_apps(config)

    log.info("Deploying Container App infrastructure for deployer job...")
    deploy_lfo_deployer(config)

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
            resource_tag_filters=args.resource_tag_filters,
            pii_scrubber_rules=args.pii_scrubber_rules,
            datadog_telemetry=args.datadog_telemetry,
            log_level=args.log_level,
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
        run_initial_deploy(
            config.deployer_job_name,
            config.control_plane_rg,
            config.control_plane_sub_id,
        )
        log.info("Initial deployment triggered")

        log_header("Success! Azure Automated Log Forwarding installation completed!")

    except Exception as e:
        log.error(f"Failed with error: {e}")
        log.error("Check the Azure CLI output for more details")
        raise


if __name__ == "__main__":
    main()
