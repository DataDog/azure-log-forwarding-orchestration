#!/usr/bin/env python3
"""
Azure Log Forwarding Orchestration Installation Script

This script combines the functionality from the ARM template at /deploy/build/azuredeploy.json
into a Python script that deploys identities, role assignments, resources, and scripts
into a customer's Azure environment using Python and the Azure CLI.

This script is designed to be executed in Azure Cloud Shell.
"""

import argparse
import json
import subprocess
import time
from logging import INFO, WARNING, basicConfig, getLogger

# Set up logging
getLogger("azure").setLevel(WARNING)
log = getLogger("installer")

# =============================================================================
# CONFIGURATION PARAMETERS
# =============================================================================


def set_test_configuration():
    """Set test configuration values when -t flag is passed."""
    global control_plane_subscription, control_plane_resource_group, control_plane_location
    global control_plane_id, datadog_api_key, datadog_application_key, datadog_site
    global monitored_subscriptions

    control_plane_subscription = "0b62a232-b8db-4380-9da6-640f7272ed6d"
    control_plane_resource_group = "lfo_altan_onboarding"
    control_plane_location = "eastus"
    control_plane_id = "altan234test"  # 12-char lowercase unique ID

    datadog_api_key = "<api-key>"
    datadog_application_key = "<app-key>"
    datadog_site = "datadoghq.com"
    monitored_subscriptions = ["0b62a232-b8db-4380-9da6-640f7272ed6d", "34464906-34fe-401e-a420-79bd0ce2a1da"]


def initialize_configuration(use_test_values: bool = False):
    """Initialize configuration parameters."""
    global control_plane_subscription, control_plane_resource_group, control_plane_location
    global control_plane_id, datadog_api_key, datadog_application_key, datadog_site
    global monitored_subscriptions, storage_account_name, control_plane_cache
    global app_service_plan, control_plane_env, deployer_job_name, container_app_start_role
    global storage_account_url, image_registry, deployer_image, function_apps

    if use_test_values:
        set_test_configuration()
    else:
        # Default configuration - update these parameters for your environment
        control_plane_subscription = "<your-subscription-id>"
        control_plane_resource_group = "dd-control-plane-rg"
        control_plane_location = "eastus"
        control_plane_id = "abcd1234efgh"  # 12-char lowercase unique ID

        # Datadog configuration
        datadog_api_key = "<your-datadog-api-key>"
        datadog_application_key = "<your-datadog-app-key>"  # Added for deployer
        datadog_site = "datadoghq.com"

        # Monitored subscriptions - update with actual subscription IDs
        monitored_subscriptions = ["<sub-id-1>", "<sub-id-2>"]

    # Derived resource names (calculated after base configuration is set)
    storage_account_name = f"lfostorage{control_plane_id}"
    control_plane_cache = "control-plane-cache"
    app_service_plan = f"control-plane-asp-{control_plane_id}"
    control_plane_env = f"deployer-task-env-{control_plane_id}"
    deployer_job_name = f"deployer-task-{control_plane_id}"
    container_app_start_role = f"ContainerAppStartRole{control_plane_id}"
    storage_account_url = f"https://{storage_account_name}.blob.core.windows.net"

    # Container configuration
    image_registry = "datadoghq.azurecr.io"
    deployer_image = f"{image_registry}/deployer:latest"

    function_apps = {
        "resources": f"resources-task-{control_plane_id}",
        "diagnostic": f"diagnostic-settings-task-{control_plane_id}",
        "scaling": f"scaling-task-{control_plane_id}",
    }


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Azure Log Forwarding Orchestration Installation Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-t", "--test", action="store_true", help="Use test configuration values instead of default placeholders"
    )

    return parser.parse_args()


# Initialize configuration variables as globals with default values
control_plane_subscription = ""
control_plane_resource_group = ""
control_plane_location = ""
control_plane_id = ""
datadog_api_key = ""
datadog_application_key = ""
datadog_site = ""
monitored_subscriptions = []
storage_account_name = ""
control_plane_cache = ""
app_service_plan = ""
control_plane_env = ""
deployer_job_name = ""
container_app_start_role = ""
storage_account_url = ""
image_registry = ""
deployer_image = ""
function_apps = {}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================


def run_cli(command: list[str]) -> str:
    """Run a shell command and return output or raise error."""
    log.debug(f"Running: {' '.join(command)}")
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        log.error(f"Command failed: {' '.join(command)}")
        log.error(result.stderr)
        raise RuntimeError(f"Command failed: {' '.join(command)}")
    return result.stdout


# =============================================================================
# STEP 1: BASIC RESOURCE SETUP
# Source: scripts/lifecycle/01_install_param.py
# =============================================================================


def set_subscription():
    """Set the active Azure subscription."""
    log.info(f"Setting active subscription to {control_plane_subscription}")
    run_cli(["az", "account", "set", "--subscription", control_plane_subscription])


def create_resource_group():
    """Create the control plane resource group."""
    log.info(f"Creating resource group {control_plane_resource_group} in {control_plane_location}")
    run_cli(["az", "group", "create", "--name", control_plane_resource_group, "--location", control_plane_location])


def create_storage_account():
    """Create the storage account for the control plane."""
    log.info(f"Creating storage account {storage_account_name}")
    run_cli(
        [
            "az",
            "storage",
            "account",
            "create",
            "--name",
            storage_account_name,
            "--resource-group",
            control_plane_resource_group,
            "--location",
            control_plane_location,
            "--sku",
            "Standard_LRS",
            "--kind",
            "StorageV2",
            "--access-tier",
            "Hot",
        ]
    )


def get_storage_key() -> str:
    """Get the storage account primary key."""
    log.debug(f"Retrieving storage account key for {storage_account_name}")
    output = run_cli(
        [
            "az",
            "storage",
            "account",
            "keys",
            "list",
            "--account-name",
            storage_account_name,
            "--resource-group",
            control_plane_resource_group,
        ]
    )
    keys = json.loads(output)
    return keys[0]["value"]


def create_blob_container(account_key: str):
    """Create blob container in the storage account."""
    log.info(f"Creating blob container {control_plane_cache}")
    run_cli(
        [
            "az",
            "storage",
            "container",
            "create",
            "--account-name",
            storage_account_name,
            "--account-key",
            account_key,
            "--name",
            control_plane_cache,
        ]
    )


def create_file_share(account_key: str):
    """Create file share in the storage account."""
    log.info(f"Creating file share {control_plane_cache}")
    run_cli(
        [
            "az",
            "storage",
            "share-rm",
            "create",
            "--storage-account",
            storage_account_name,
            "--name",
            control_plane_cache,
        ]
    )


# =============================================================================
# STEP 2: DATADOG API KEY VALIDATION
# Source: scripts/lifecycle/02_api_key_valid.py
# =============================================================================


def validate_datadog_api_key():
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


def create_app_service_plan():
    """Create the App Service Plan for Function Apps."""
    log.info(f"Creating App Service Plan {app_service_plan}")
    run_cli(
        [
            "az",
            "functionapp",
            "plan",
            "create",
            "--resource-group",
            control_plane_resource_group,
            "--name",
            app_service_plan,
            "--location",
            control_plane_location,
            "--number-of-workers",
            "1",
            "--sku",
            "Y1",
            "--is-linux",
        ]
    )


def create_function_app(name: str, key: str):
    """Create a Function App with required configuration."""
    log.info(f"Creating Function App {name}")
    run_cli(
        [
            "az",
            "functionapp",
            "create",
            "--resource-group",
            control_plane_resource_group,
            "--consumption-plan-location",
            control_plane_location,
            "--runtime",
            "python",
            "--functions-version",
            "4",
            "--os-type",
            "Linux",
            "--name",
            name,
            "--storage-account",
            storage_account_name,
            "--plan",
            app_service_plan,
            "--assign-identity",
        ]
    )

    # Add app settings (simulating what's in ARM)
    log.debug(f"Configuring app settings for Function App {name}")
    run_cli(
        [
            "az",
            "functionapp",
            "config",
            "appsettings",
            "set",
            "--name",
            name,
            "--resource-group",
            control_plane_resource_group,
            "--settings",
            f"AzureWebJobsStorage=DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={key}",
            "FUNCTIONS_EXTENSION_VERSION=~4",
            "FUNCTIONS_WORKER_RUNTIME=python",
            f"WEBSITE_CONTENTAZUREFILECONNECTIONSTRING=DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={key}",
            f"WEBSITE_CONTENTSHARE={name}",
            "AzureWebJobsFeatureFlags=EnableWorkerIndexing",
        ]
    )


def create_function_apps():
    """Create all required Function Apps."""
    log.info("Creating App Service Plan...")
    create_app_service_plan()

    log.info("Fetching storage key...")
    key = get_storage_key()

    log.info("Creating Function Apps...")
    for role, app_name in function_apps.items():
        log.info(f"Creating Function App: {app_name}")
        create_function_app(app_name, key)

    log.info("Function Apps created and configured")


# =============================================================================
# STEP 4: CONTAINER APP ENVIRONMENT AND DEPLOYER JOB
# Source: scripts/lifecycle/04_deployer.py
# =============================================================================


def create_user_assigned_identity():
    """Create a user-assigned managed identity."""
    log.info("Creating user-assigned managed identity")
    run_cli(
        [
            "az",
            "identity",
            "create",
            "--name",
            "runInitialDeployIdentity",
            "--resource-group",
            control_plane_resource_group,
            "--location",
            control_plane_location,
        ]
    )


def create_containerapp_environment():
    """Create the Container App environment."""
    log.info(f"Creating Container App environment {control_plane_env}")
    run_cli(
        [
            "az",
            "containerapp",
            "env",
            "create",
            "--name",
            control_plane_env,
            "--resource-group",
            control_plane_resource_group,
            "--location",
            control_plane_location,
        ]
    )


def create_containerapp_job():
    """Create the Container App job for the deployer."""
    log.info(f"Creating Container App job {deployer_job_name}")
    storage_key = get_storage_key()

    run_cli(
        [
            "az",
            "containerapp",
            "job",
            "create",
            "--name",
            deployer_job_name,
            "--resource-group",
            control_plane_resource_group,
            "--environment",
            control_plane_env,
            "--replica-timeout",
            "1800",
            "--replica-retry-limit",
            "1",
            "--trigger-type",
            "Schedule",
            "--cron-expression",
            "*/30 * * * *",
            "--image",
            deployer_image,
            "--cpu",
            "0.5",
            "--memory",
            "1Gi",
            "--assign-identity",
            "--env-vars",
            f"SUBSCRIPTION_ID={control_plane_subscription}",
            f"RESOURCE_GROUP={control_plane_resource_group}",
            f"REGION={control_plane_location}",
            "DD_API_KEY=secretref:dd-api-key",
            "DD_APP_KEY=secretref:dd-app-key",
            f"DD_SITE={datadog_site}",
            "AzureWebJobsStorage=secretref:connection-string",
            f"STORAGE_ACCOUNT_URL={storage_account_url}",
            "--secrets",
            f"connection-string=DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_key}",
            f"dd-api-key={datadog_api_key}",
            f"dd-app-key={datadog_application_key}",
        ]
    )


def create_custom_role_definition():
    """Create a custom role for starting container app jobs."""
    log.info(f"Creating custom role definition {container_app_start_role}")
    scope = run_cli(
        ["az", "group", "show", "--name", control_plane_resource_group, "--query", "id", "--output", "tsv"]
    ).strip()

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

    run_cli(["az", "role", "definition", "create", "--role-definition", "custom_role.json"])


def assign_custom_role_to_identity():
    """Assign the custom role to the managed identity."""
    log.info("Assigning custom role to managed identity")
    identity_id = run_cli(
        [
            "az",
            "identity",
            "show",
            "--name",
            "runInitialDeployIdentity",
            "--resource-group",
            control_plane_resource_group,
            "--query",
            "principalId",
            "--output",
            "tsv",
        ]
    ).strip()

    scope = run_cli(
        ["az", "group", "show", "--name", control_plane_resource_group, "--query", "id", "--output", "tsv"]
    ).strip()

    role_id = run_cli(
        [
            "az",
            "role",
            "definition",
            "list",
            "--name",
            container_app_start_role,
            "--query",
            "[0].name",
            "--output",
            "tsv",
        ]
    ).strip()

    run_cli(
        [
            "az",
            "role",
            "assignment",
            "create",
            "--role",
            role_id,
            "--assignee-object-id",
            identity_id,
            "--assignee-principal-type",
            "ServicePrincipal",
            "--scope",
            scope,
        ]
    )


def deploy_container_job_infra():
    """Deploy all container job infrastructure."""
    log.info("Creating managed identity...")
    create_user_assigned_identity()

    log.info("Creating container app environment...")
    create_containerapp_environment()

    log.info("Creating container app job...")
    create_containerapp_job()

    log.info("Defining custom role...")
    create_custom_role_definition()

    log.info("Assigning custom role to identity...")
    assign_custom_role_to_identity()

    log.info("Container App job + identity setup complete")


# =============================================================================
# STEP 5: INITIAL DEPLOYMENT TRIGGER
# Source: scripts/lifecycle/05_initial_job_trigger.py
# =============================================================================


def run_initial_deploy_script():
    """Trigger the initial deployment using deployment scripts."""
    log.info("Starting initial container app job via deployment script...")

    # Get the full identity resource ID
    identity_id = run_cli(
        [
            "az",
            "identity",
            "show",
            "--name",
            "runInitialDeployIdentity",
            "--resource-group",
            control_plane_resource_group,
            "--query",
            "id",
            "--output",
            "tsv",
        ]
    ).strip()

    # Get the storage key again
    storage_key = get_storage_key()

    # Build PowerShell script content
    ps_script = f"Start-AzContainerAppJob -Name {deployer_job_name} -ResourceGroupName {control_plane_resource_group}"

    run_cli(
        [
            "az",
            "deployment-scripts",
            "create",
            "--name",
            "runInitialDeploy",
            "--resource-group",
            control_plane_resource_group,
            "--location",
            control_plane_location,
            "--script-name",
            "runInitialDeploy",
            "--script-content",
            ps_script,
            "--az-powershell-version",
            "12.3",
            "--storage-account-name",
            storage_account_name,
            "--storage-account-key",
            storage_key,
            "--cleanup-preference",
            "OnSuccess",
            "--retention-interval",
            "PT1H",
            "--identity-type",
            "UserAssigned",
            "--user-assigned-identities",
            identity_id,
        ]
    )

    log.info("Initial deployment script executed")


# =============================================================================
# STEP 6: RBAC PERMISSIONS ACROSS SUBSCRIPTIONS
# Source: scripts/lifecycle/06_monitored_subs_roles.py
# =============================================================================


def get_function_principal_id(function_app_name: str) -> str:
    """Get the principal ID of a Function App's managed identity."""
    log.debug(f"Getting principal ID for Function App {function_app_name}")
    output = run_cli(
        [
            "az",
            "functionapp",
            "identity",
            "show",
            "--name",
            function_app_name,
            "--resource-group",
            control_plane_resource_group,
            "--query",
            "principalId",
            "--output",
            "tsv",
        ]
    )
    return output.strip()


def assign_role(scope: str, principal_id: str, role_id: str):
    """Assign a role to a principal at a given scope."""
    log.debug(f"Assigning role {role_id} to principal {principal_id} at scope {scope}")
    run_cli(
        [
            "az",
            "role",
            "assignment",
            "create",
            "--assignee-object-id",
            principal_id,
            "--assignee-principal-type",
            "ServicePrincipal",
            "--role",
            role_id,
            "--scope",
            scope,
        ]
    )


def grant_subscription_permissions():
    """Grant permissions across all monitored subscriptions."""
    log.info("Setting up permissions across monitored subscriptions...")

    # Get principal IDs for function apps
    resource_task_pid = get_function_principal_id(function_apps["resources"])
    diagnostic_pid = get_function_principal_id(function_apps["diagnostic"])
    scaling_pid = get_function_principal_id(function_apps["scaling"])

    for sub_id in monitored_subscriptions:
        log.info(f"Assigning permissions in subscription: {sub_id}")

        # Set context to target subscription
        run_cli(["az", "account", "set", "--subscription", sub_id])

        # Create RG in target subscription if it doesn't exist
        run_cli(["az", "group", "create", "--name", control_plane_resource_group, "--location", control_plane_location])

        # Get scope
        subscription_scope = f"/subscriptions/{sub_id}"
        resource_group_scope = f"{subscription_scope}/resourceGroups/{control_plane_resource_group}"

        # Assign roles at subscription level
        assign_role(
            subscription_scope,
            resource_task_pid,
            "43d0d8ad-25c7-4714-9337-8ba259a9fe05",  # Reader role
        )
        assign_role(
            subscription_scope,
            diagnostic_pid,
            "749f88d5-cbae-40b8-bcfc-e573ddc772fa",  # Monitoring Reader
        )

        # Assign roles at resource group level
        assign_role(
            resource_group_scope,
            diagnostic_pid,
            "c12c1c16-33a1-487b-954d-41c89c60f349",  # Storage blob reader for diagnostics
        )
        assign_role(
            resource_group_scope,
            scaling_pid,
            "b24988ac-6180-42a0-ab88-20f7382dd24c",  # Contributor (used for scaling)
        )

    # Reset back to control plane subscription
    run_cli(["az", "account", "set", "--subscription", control_plane_subscription])
    log.info("Subscription permission setup complete")


# =============================================================================
# MAIN INSTALLATION FLOW
# =============================================================================


def main():
    """Main installation flow that orchestrates all steps."""
    log.info("Starting Azure Log Forwarding Orchestration Installation...")
    log.info("=" * 70)

    try:
        # Step 1: Basic resource setup
        log.info("STEP 1: Setting up basic resources...")
        set_subscription()
        create_resource_group()
        create_storage_account()
        log.info("Waiting for storage account to be ready...")
        time.sleep(10)  # Ensure the storage account is ready
        key = get_storage_key()
        create_blob_container(key)
        create_file_share(key)
        log.info("Storage and resource group setup completed")

        # Step 2: Validate Datadog configuration
        log.info("STEP 2: Validating Datadog configuration...")
        validate_datadog_api_key()

        # Step 3: Create Function Apps
        log.info("STEP 3: Creating Function Apps...")
        create_function_apps()

        # Step 4: Deploy Container infrastructure
        log.info("STEP 4: Deploying Container App infrastructure...")
        deploy_container_job_infra()

        # Step 5: Trigger initial deployment
        log.info("STEP 5: Triggering initial deployment...")
        run_initial_deploy_script()

        # Step 6: Set up cross-subscription permissions
        log.info("STEP 6: Setting up cross-subscription permissions...")
        grant_subscription_permissions()

        log.info("=" * 70)
        log.info("Azure Log Forwarding Orchestration installation completed successfully!")
        log.info("Check the Azure portal to verify all resources were created")

    except Exception as e:
        log.error(f"Installation failed with error: {e}")
        log.error("Check the Azure CLI output above for more details")
        raise


if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()

    # Initialize configuration based on arguments
    initialize_configuration(use_test_values=args.test)

    # Set up logging and run main function
    basicConfig(level=INFO)
    main()
