#!/usr/bin/env python3
"""
Deploy personal forwarder to Azure VM.

This script:
1. Creates Azure resources (resource group, storage, VM) if they don't exist
2. Builds the forwarder binary for Linux
3. Uploads the binary to Azure Storage
4. Deploys to VM via SSH with systemd timer configuration

Similar to deploy_personal_env.py but for VM-based deployment.
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from typing import Any
from pathlib import Path

from azure.identity import AzureCliCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobServiceClient


# Configuration
LOCATION = "eastus"
VM_SIZE = "Standard_B2s"  # 2 vCPUs, 4GB RAM - matches container app specs
VM_IMAGE = {
    "publisher": "Canonical",
    "offer": "0001-com-ubuntu-server-jammy",
    "sku": "22_04-lts-gen2",
    "version": "latest",
}


def run(cmd: str | list[str], capture_output: bool = True, **kwargs: Any) -> str:
    """Run a command and return output."""
    if isinstance(cmd, str):
        cmd = cmd.split()

    print(f"Running: {' '.join(cmd)}")

    if capture_output:
        result = subprocess.run(cmd, capture_output=True, text=True, **kwargs)
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {' '.join(cmd)}")
        return result.stdout.strip()
    else:
        result = subprocess.run(cmd, **kwargs)
        if result.returncode != 0:
            raise Exception(f"Command failed: {' '.join(cmd)}")
        return ""


def get_name(name: str, max_length: int) -> str:
    """
    Get a name that fits within Azure's length constraints.
    If name exceeds max_length, truncate and append MD5 hash.
    """
    if len(name) <= max_length:
        return name

    # Truncate and add hash for uniqueness
    hash_str = hashlib.md5(name.encode()).hexdigest()[:6]
    truncated = name[: max_length - 7]  # Leave room for hash and separator
    return f"{truncated}-{hash_str}"


def get_version_tag() -> str:
    """
    Generate version tag from git commit.
    Returns short SHA if clean, or SHA-dirty if uncommitted changes.
    """
    try:
        # Get short commit SHA (8 chars by default)
        commit_sha = run("git rev-parse --short HEAD")

        # Check for uncommitted changes
        status = run("git status --porcelain")

        if status:
            return f"{commit_sha}-dirty"
        return commit_sha
    except Exception:
        print("Warning: Could not get git version, using 'unknown'")
        return "unknown"


def get_ssh_public_key() -> str:
    """Get SSH public key for VM authentication."""
    ssh_key_path = os.path.expanduser("~/.ssh/id_rsa.pub")
    if not os.path.exists(ssh_key_path):
        raise Exception(f"SSH public key not found at {ssh_key_path}. Please generate one with: ssh-keygen -t rsa")

    with open(ssh_key_path, "r") as f:
        return f.read().strip()


def create_resource_group(resource_client: ResourceManagementClient, name: str) -> None:
    """Create resource group if it doesn't exist."""
    if not resource_client.resource_groups.check_existence(name):
        print(f"Creating resource group: {name}")
        resource_client.resource_groups.create_or_update(name, {"location": LOCATION})
    else:
        print(f"Resource group {name} already exists")


def create_storage_account(storage_client: StorageManagementClient, resource_group: str, name: str) -> str:
    """Create storage account if it doesn't exist and return connection string."""
    availability = storage_client.storage_accounts.check_name_availability({"name": name})

    if availability.name_available:
        print(f"Creating storage account: {name}")
        poller = storage_client.storage_accounts.begin_create(
            resource_group,
            name,
            {
                "location": LOCATION,
                "kind": "StorageV2",
                "sku": {"name": "Standard_LRS"},
                "properties": {
                    "allowBlobPublicAccess": False,  # Comply with security policy
                    "supportsHttpsTrafficOnly": True,  # Enable HTTPS only
                    "minimumTlsVersion": "TLS1_2",
                },
            },
        )
        poller.result()
        print(f"Storage account {name} created")

        # Wait for propagation
        time.sleep(20)
    else:
        print(f"Storage account {name} already exists")

    # Get connection string
    keys = storage_client.storage_accounts.list_keys(resource_group, name)
    key = keys.keys[0].value
    connection_string = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={name};"
        f"AccountKey={key};"
        f"EndpointSuffix=core.windows.net"
    )

    return connection_string


def create_storage_container(connection_string: str, container_name: str) -> None:
    """Create storage container if it doesn't exist."""
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service.get_container_client(container_name)

    if not container_client.exists():
        print(f"Creating storage container: {container_name}")
        container_client.create_container()
    else:
        print(f"Storage container {container_name} already exists")


def create_network_resources(network_client: NetworkManagementClient, resource_group: str, base_name: str) -> dict:
    """Create VNet, Subnet, NSG, Public IP, and NIC for VM."""
    resources = {}

    # Create Virtual Network
    vnet_name = f"{base_name}-vnet"
    print(f"Creating virtual network: {vnet_name}")

    vnet_params = {"location": LOCATION, "address_space": {"address_prefixes": ["10.0.0.0/16"]}}

    vnet_poller = network_client.virtual_networks.begin_create_or_update(resource_group, vnet_name, vnet_params)
    vnet = vnet_poller.result()
    resources["vnet"] = vnet

    # Create Subnet
    subnet_name = "default"
    print(f"Creating subnet: {subnet_name}")

    subnet_params = {"address_prefix": "10.0.1.0/24"}

    subnet_poller = network_client.subnets.begin_create_or_update(resource_group, vnet_name, subnet_name, subnet_params)
    subnet = subnet_poller.result()
    resources["subnet"] = subnet

    # Create Network Security Group
    nsg_name = f"{base_name}-nsg"
    print(f"Creating network security group: {nsg_name}")

    nsg_params = {
        "location": LOCATION,
        "security_rules": [
            {
                "name": "SSH",
                "priority": 1000,
                "access": "Allow",
                "direction": "Inbound",
                "protocol": "Tcp",
                "source_port_range": "*",
                "destination_port_range": "22",
                "source_address_prefix": "*",
                "destination_address_prefix": "*",
            }
        ],
    }

    nsg_poller = network_client.network_security_groups.begin_create_or_update(resource_group, nsg_name, nsg_params)
    nsg = nsg_poller.result()
    resources["nsg"] = nsg

    # Create Public IP
    public_ip_name = f"{base_name}-ip"
    print(f"Creating public IP: {public_ip_name}")

    public_ip_params = {
        "location": LOCATION,
        "sku": {"name": "Standard"},
        "public_ip_allocation_method": "Static",
        "public_ip_address_version": "IPv4",
    }

    ip_poller = network_client.public_ip_addresses.begin_create_or_update(
        resource_group, public_ip_name, public_ip_params
    )
    public_ip = ip_poller.result()
    resources["public_ip"] = public_ip

    # Create Network Interface
    nic_name = f"{base_name}-nic"
    print(f"Creating network interface: {nic_name}")

    nic_params = {
        "location": LOCATION,
        "ip_configurations": [
            {"name": "ipconfig1", "subnet": {"id": subnet.id}, "public_ip_address": {"id": public_ip.id}}
        ],
        "network_security_group": {"id": nsg.id},
    }

    nic_poller = network_client.network_interfaces.begin_create_or_update(resource_group, nic_name, nic_params)
    nic = nic_poller.result()
    resources["nic"] = nic

    return resources


def create_vm(
    compute_client: ComputeManagementClient, resource_group: str, vm_name: str, nic_id: str, ssh_key: str
) -> None:
    """Create Ubuntu VM if it doesn't exist."""
    # Check if VM exists
    try:
        compute_client.virtual_machines.get(resource_group, vm_name)
        print(f"VM {vm_name} already exists")
        return
    except Exception:
        pass  # VM doesn't exist, create it

    print(f"Creating VM: {vm_name}")

    vm_params = {
        "location": LOCATION,
        "hardware_profile": {"vm_size": VM_SIZE},
        "storage_profile": {
            "image_reference": VM_IMAGE,
            "os_disk": {"create_option": "FromImage", "managed_disk": {"storage_account_type": "Standard_LRS"}},
        },
        "os_profile": {
            "computer_name": vm_name,
            "admin_username": "azureuser",
            "linux_configuration": {
                "disable_password_authentication": True,
                "ssh": {"public_keys": [{"path": "/home/azureuser/.ssh/authorized_keys", "key_data": ssh_key}]},
            },
        },
        "network_profile": {"network_interfaces": [{"id": nic_id, "primary": True}]},
    }

    vm_poller = compute_client.virtual_machines.begin_create_or_update(resource_group, vm_name, vm_params)
    vm_poller.result()
    print(f"VM {vm_name} created successfully")


def get_vm_public_ip(network_client: NetworkManagementClient, resource_group: str, ip_name: str) -> str:
    """Get the public IP address of the VM."""
    public_ip = network_client.public_ip_addresses.get(resource_group, ip_name)
    return public_ip.ip_address


def build_forwarder_binary(version_tag: str) -> str:
    """Build the forwarder binary for Linux and return the path."""
    print(f"Building forwarder binary with version: {version_tag}")

    # Change to forwarder directory
    forwarder_dir = Path(__file__).parent.parent / "forwarder"
    os.chdir(forwarder_dir)

    # Build binary
    output_file = f"forwarder-linux-amd64-{version_tag}"
    build_cmd = [
        "go",
        "build",
        "-ldflags",
        f"-s -w -X main.version={version_tag}",
        "-o",
        output_file,
        "cmd/forwarder/forwarder.go",
    ]

    env = os.environ.copy()
    env["CGO_ENABLED"] = "0"
    env["GOOS"] = "linux"
    env["GOARCH"] = "amd64"

    run(build_cmd, capture_output=False, env=env)

    binary_path = forwarder_dir / output_file
    print(f"Binary built at: {binary_path}")

    # Calculate checksum
    with open(binary_path, "rb") as f:
        checksum = hashlib.sha256(f.read()).hexdigest()

    print(f"Binary checksum: {checksum}")

    # Write checksum file (filename should match what will be downloaded)
    checksum_file = binary_path.with_suffix(".sha256")
    with open(checksum_file, "w") as f:
        f.write(f"{checksum}  forwarder-{version_tag}\n")

    return str(binary_path)


def upload_binary_to_storage(connection_string: str, version_tag: str, binary_path: str) -> str:
    """Upload forwarder binary to Azure Storage and return the URL."""
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    container_name = "forwarder"

    # Ensure container exists
    create_storage_container(connection_string, container_name)

    # Upload binary
    blob_name = f"{version_tag}/forwarder-linux-amd64"
    print(f"Uploading binary to: {container_name}/{blob_name}")

    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)

    with open(binary_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    # Upload checksum
    checksum_blob_name = f"{version_tag}/forwarder-linux-amd64.sha256"
    checksum_path = f"{binary_path}.sha256"

    checksum_blob_client = blob_service.get_blob_client(container=container_name, blob=checksum_blob_name)

    with open(checksum_path, "rb") as data:
        checksum_blob_client.upload_blob(data, overwrite=True)

    print("Binary uploaded successfully")
    return blob_name


def deploy_to_vm(
    vm_ip: str,
    connection_string: str,
    version_tag: str,
    dd_api_key: str,
    control_plane_id: str,
    config_id: str,
    dd_site: str = "datadoghq.com",
) -> None:
    """Deploy forwarder to VM via SSH."""
    print(f"Deploying to VM at {vm_ip}")
    print(f"Using DD_SITE: {dd_site}")

    # Prepare environment variables
    env_vars = {
        "AzureWebJobsStorage": connection_string,
        "DD_API_KEY": dd_api_key,
        "DD_SITE": dd_site,
        "CONTROL_PLANE_ID": control_plane_id,
        "CONFIG_ID": config_id,
        "VERSION_TAG": version_tag,
        "NUM_GOROUTINES": os.getenv("NUM_GOROUTINES", "10"),
        "DD_TELEMETRY": os.getenv("DD_TELEMETRY", "true"),
        "DD_APM_ENABLED": os.getenv("DD_APM_ENABLED", "false"),
        "PII_SCRUBBER_RULES": os.getenv("PII_SCRUBBER_RULES", ""),
    }

    # Copy deployment scripts to VM
    script_dir = Path(__file__).parent / "forwarder-vm-deployment"

    # Create scripts directory on VM
    run(
        ["ssh", "-o", "StrictHostKeyChecking=no", f"azureuser@{vm_ip}", "mkdir -p $HOME/deployment"],
        capture_output=False,
    )

    # Copy scripts
    print("Copying deployment scripts to VM...")
    for script in ["initial_setup.sh", "deploy.sh", "update.sh", "rollback.sh"]:
        script_path = script_dir / script
        if script_path.exists():
            run(f"scp -o StrictHostKeyChecking=no {script_path} azureuser@{vm_ip}:deployment/", capture_output=False)

    # Copy systemd files
    configs_dir = Path(__file__).parent.parent / "configs" / "systemd"
    for config in ["datadog-forwarder.service", "datadog-forwarder.timer"]:
        config_path = configs_dir / config
        if config_path.exists():
            run(f"scp -o StrictHostKeyChecking=no {config_path} azureuser@{vm_ip}:deployment/", capture_output=False)

    # Run initial setup
    print("Running initial setup on VM...")
    run(
        [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            f"azureuser@{vm_ip}",
            "chmod +x $HOME/deployment/*.sh && $HOME/deployment/initial_setup.sh",
        ],
        capture_output=False,
    )

    # Create environment file
    env_content = "\n".join([f'{k}="{v}"' for k, v in env_vars.items() if v])
    env_file = "/tmp/forwarder-environment"
    with open(env_file, "w") as f:
        f.write(env_content)

    # Copy environment file
    run(
        ["scp", "-o", "StrictHostKeyChecking=no", env_file, f"azureuser@{vm_ip}:deployment/environment"],
        capture_output=False,
    )
    run(
        [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            f"azureuser@{vm_ip}",
            "sudo mv $HOME/deployment/environment /etc/datadog-forwarder/environment && sudo chmod 600 /etc/datadog-forwarder/environment",
        ],
        capture_output=False,
    )

    # Deploy the binary
    print(f"Deploying version {version_tag}...")
    run(
        [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            f"azureuser@{vm_ip}",
            f"sudo $HOME/deployment/deploy.sh '{connection_string}' {version_tag}",
        ],
        capture_output=False,
    )

    print("Deployment complete!")

    # Clean up temp file
    os.remove(env_file)


def main():
    parser = argparse.ArgumentParser(description="Deploy forwarder to Azure VM")
    parser.add_argument("--base-name", default=None, help="Base name for resources (default: lfo<username>vm)")
    parser.add_argument("--skip-build", action="store_true", help="Skip building the binary")
    parser.add_argument("--skip-upload", action="store_true", help="Skip uploading to storage")
    parser.add_argument("--skip-deploy", action="store_true", help="Skip deployment to VM")
    parser.add_argument("--subscription-id", default=None, help="Azure subscription ID")
    args = parser.parse_args()

    # Check required environment variables
    dd_api_key = os.getenv("DD_API_KEY")
    if not dd_api_key:
        print("Error: DD_API_KEY environment variable is required")
        sys.exit(1)

    control_plane_id = os.getenv("CONTROL_PLANE_ID")
    if not control_plane_id:
        print("Error: CONTROL_PLANE_ID environment variable is required")
        sys.exit(1)

    config_id = os.getenv("CONFIG_ID")
    if not config_id:
        print("Error: CONFIG_ID environment variable is required")
        sys.exit(1)

    # Get DD_SITE from environment (should be set in ~/.profile)
    dd_site = os.getenv("DD_SITE", "datadoghq.com")
    if not os.getenv("DD_SITE"):
        print("Warning: DD_SITE not found in environment, using default: datadoghq.com")
        print("         To set DD_SITE, add 'export DD_SITE=\"datadoghq.com\"' to ~/.profile and source it")
    else:
        print(f"Using DD_SITE from environment: {dd_site}")

    # Get configuration
    username = os.getenv("USER", "unknown")
    base_name = args.base_name or os.getenv("LFO_VM_BASE_NAME", f"lfo{username}vm")

    # Get version tag
    version_tag = get_version_tag()
    print(f"Version tag: {version_tag}")

    # Initialize Azure clients
    credential = AzureCliCredential()

    # Get subscription ID
    if args.subscription_id:
        subscription_id = args.subscription_id
    else:
        subscription_id = run("az account show --query id -o tsv")

    print(f"Using subscription: {subscription_id}")

    # Set subscription
    run(f"az account set --subscription {subscription_id}")

    # Create clients
    resource_client = ResourceManagementClient(credential, subscription_id)
    storage_client = StorageManagementClient(credential, subscription_id)
    network_client = NetworkManagementClient(credential, subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)

    # Generate resource names
    resource_group_name = get_name(f"{base_name}rg", 90)
    storage_account_name = get_name(f"{base_name}storage".replace("-", "").lower(), 24)
    vm_name = get_name(f"{base_name}", 64)

    print("Resource names:")
    print(f"  Resource Group: {resource_group_name}")
    print(f"  Storage Account: {storage_account_name}")
    print(f"  VM: {vm_name}")

    # Create resource group
    create_resource_group(resource_client, resource_group_name)

    # Create storage account
    connection_string = create_storage_account(storage_client, resource_group_name, storage_account_name)

    # Create network resources
    network_resources = create_network_resources(network_client, resource_group_name, base_name)

    # Get SSH key
    ssh_key = get_ssh_public_key()

    # Create VM
    create_vm(compute_client, resource_group_name, vm_name, network_resources["nic"].id, ssh_key)

    # Output resource IDs
    storage_resource_id = f"/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.Storage/storageAccounts/{storage_account_name}"
    vm_resource_id = f"/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.Compute/virtualMachines/{vm_name}"

    print(f"Storage Account Resource ID: {storage_resource_id}")
    print(f"VM Resource ID: {vm_resource_id}")

    # Get VM public IP
    public_ip_name = f"{base_name}-ip"
    vm_ip = get_vm_public_ip(network_client, resource_group_name, public_ip_name)
    print(f"VM public IP: {vm_ip}")

    # Build forwarder binary
    if not args.skip_build:
        binary_path = build_forwarder_binary(version_tag)
    else:
        # Assume binary exists
        forwarder_dir = Path(__file__).parent.parent / "forwarder"
        binary_path = str(forwarder_dir / f"forwarder-linux-amd64-{version_tag}")
        if not os.path.exists(binary_path):
            print(f"Error: Binary not found at {binary_path}")
            sys.exit(1)

    # Upload to storage
    if not args.skip_upload:
        upload_binary_to_storage(connection_string, version_tag, binary_path)

    # Deploy to VM
    if not args.skip_deploy:
        # Wait a bit for VM to be fully ready
        print("Waiting for VM to be fully ready...")
        time.sleep(30)

        deploy_to_vm(vm_ip, connection_string, version_tag, dd_api_key, control_plane_id, config_id, dd_site)

    # Deploy loggy function app
    print("\n=== Deploying Loggy Function App ===")
    function_app_name = f"{base_name}-loggy"[:60]  # Max 60 chars for function app name

    # Check if function app exists
    existing_apps_output = run(f"az functionapp list --resource-group {resource_group_name} --output json")
    existing_apps = json.loads(existing_apps_output if existing_apps_output else "[]")
    app_exists = any(app.get("name") == function_app_name for app in existing_apps)

    if not app_exists:
        print(f"Creating function app {function_app_name}...")

        # Try to create with consumption plan first (no quota needed)
        try:
            print("Attempting to create function app with consumption plan...")
            run(
                f"az functionapp create --name {function_app_name} --storage-account {storage_account_name} --resource-group {resource_group_name} --consumption-plan-location eastus --runtime python --runtime-version 3.11 --functions-version 4 --os-type Linux --https-only true",
                capture_output=False,
                check=True,
            )
            print(f"Function app {function_app_name} created with consumption plan")
        except Exception as e:
            print(f"Consumption plan creation failed: {e}")
            print("Attempting to create function app with Basic (B1) app service plan...")

            # Try with B1 plan as fallback
            try:
                plan_name = f"{base_name}-plan"
                # Create App Service Plan
                run(
                    f"az appservice plan create --name {plan_name} --resource-group {resource_group_name} --location eastus --sku B1 --is-linux",
                    capture_output=False,
                    check=True,
                )

                # Create function app
                run(
                    f"az functionapp create --name {function_app_name} --storage-account {storage_account_name} --resource-group {resource_group_name} --plan {plan_name} --runtime python --runtime-version 3.11 --functions-version 4 --os-type linux --https-only",
                    capture_output=False,
                    check=True,
                )
                print(f"Function app {function_app_name} created with B1 app service plan")
            except Exception as e2:
                print(f"WARNING: Could not create function app: {e2}")
                print(
                    "The forwarder will still work, but you won't be able to generate test logs via the function app."
                )
    else:
        print(f"Function app {function_app_name} already exists")

    # Check again if function app was created successfully
    existing_apps_output = run(f"az functionapp list --resource-group {resource_group_name} --output json")
    existing_apps = json.loads(existing_apps_output if existing_apps_output else "[]")
    app_exists = any(app.get("name") == function_app_name for app in existing_apps)

    if app_exists:
        # Deploy loggy code
        print(f"Deploying loggy code to {function_app_name}...")
        try:
            loggy_path = Path(__file__).parent.parent / "loggy"
            run(
                f"func azure functionapp publish {function_app_name} --python",
                capture_output=False,
                cwd=str(loggy_path),
            )
            print(f"Loggy deployed successfully to {function_app_name}")
            print(f"Function app URL: https://{function_app_name}.azurewebsites.net")
        except Exception as e:
            print(f"WARNING: Could not deploy loggy code: {e}")
            print("The function app exists but code deployment failed. You may need to deploy manually.")
    else:
        print(f"WARNING: Function app {function_app_name} was not created.")
        print("The forwarder will still work, but you won't be able to generate test logs via the function app.")

    # Configure diagnostic settings for loggy to send logs to the storage account (only if function app exists)
    if app_exists:
        print(f"\nConfiguring diagnostic settings for {function_app_name} to forward logs to storage account...")
        function_app_resource_id = f"/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.Web/sites/{function_app_name}"
        storage_resource_id = f"/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.Storage/storageAccounts/{storage_account_name}"

        # Create diagnostic setting to send all logs to storage account
        diagnostic_setting_name = "datadog-lfo"

        # First check if diagnostic setting already exists
        existing_settings = run(
            f"az monitor diagnostic-settings list --resource {function_app_resource_id} --output json",
            capture_output=True,
            check=False,
        )

        if existing_settings and diagnostic_setting_name not in existing_settings:
            try:
                # Create diagnostic setting with all available log categories
                run(
                    f"az monitor diagnostic-settings create "
                    f"--name {diagnostic_setting_name} "
                    f"--resource {function_app_resource_id} "
                    f"--storage-account {storage_resource_id} "
                    f'--logs \'[{{"categoryGroup": "allLogs", "enabled": true, "retentionPolicy": {{"days": 7, "enabled": true}}}}]\' '
                    f'--metrics \'[{{"category": "AllMetrics", "enabled": true, "retentionPolicy": {{"days": 7, "enabled": true}}}}]\'',
                    capture_output=False,
                )
                print(f"✅ Diagnostic settings configured - logs will be forwarded to {storage_account_name}")
            except Exception as e:
                print(f"⚠️ Warning: Could not configure diagnostic settings: {e}")
                print(f"You may need to manually configure diagnostic settings for {function_app_name}")
                print("To do this manually, run:")
                print(
                    f'  az monitor diagnostic-settings create --name {diagnostic_setting_name} --resource {function_app_resource_id} --storage-account {storage_resource_id} --logs \'[{{"categoryGroup": "allLogs", "enabled": true}}]\''
                )
        else:
            print(f"✅ Diagnostic settings already configured for {function_app_name}")

    print("\n✅ Deployment completed successfully!")
    print(
        f"\nStorage Account Resource ID:\n  /subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.Storage/storageAccounts/{storage_account_name}"
    )
    print(
        f"\nVM Resource ID:\n  /subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.Compute/virtualMachines/{vm_name}"
    )
    print(f"\nSSH to VM: ssh azureuser@{vm_ip}")
    print(f"Check logs: ssh azureuser@{vm_ip} 'sudo journalctl -u datadog-forwarder -f'")
    print(f"Check timer: ssh azureuser@{vm_ip} 'sudo systemctl status datadog-forwarder.timer'")

    # Get function key
    try:
        function_key_result = run(
            f"az functionapp function keys list --name {function_app_name} --resource-group {resource_group_name} --function-name CustomLog --query default -o tsv",
            capture_output=True,
            text=True,
            shell=True,
        )
        function_key = function_key_result.stdout.strip()
    except Exception:
        function_key = "Unable to get function key - check Azure portal"

    print("\n🚀 Test Loggy with Requesty:")
    print("# First, build requesty if you haven't already:")
    print("cd requesty && go build -o requesty cmd/requesty/main.go && cd ..")
    print("# Then test your loggy deployment:")
    print(
        f'./requesty/requesty -url https://{function_app_name}.azurewebsites.net/api/CustomLog -key "{function_key}" -duration 30s -rps 10'
    )
    print("# Or with variety mode for fun messages:")
    print(
        f'./requesty/requesty -url https://{function_app_name}.azurewebsites.net/api/CustomLog -key "{function_key}" -duration 60s -rps 50 -variety'
    )


if __name__ == "__main__":
    main()
