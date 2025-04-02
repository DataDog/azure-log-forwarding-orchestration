#!/usr/bin/env python3.11
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/  Copyright 2025 Datadog, Inc.

"""Adapted from: https://learn.microsoft.com/en-us/azure/azure-resource-manager/templates/secure-template-with-sas-token?tabs=azure-cli#provide-sas-token-during-deployment"""

from os import environ
from os.path import isfile, join
from subprocess import run
from urllib.parse import quote


def get_connection_string(account: str, rg: str) -> str:
    return run(
        [
            *("az", "storage", "account", "show-connection-string"),
            *("--resource-group", rg),
            *("--name", account),
            *("--query", "connectionString"),
        ],
        capture_output=True,
        text=True,
    ).stdout.strip()


def get_token(file: str, duration: str, connection: str) -> str:
    return run(
        [
            *("az", "storage", "blob", "generate-sas"),
            *("--container-name", "templates"),
            *("--name", file),
            *("--permissions", "r"),
            *("--expiry", duration),
            *("--connection-string", connection),
        ],
        capture_output=True,
        text=True,
    ).stdout.strip()


DEPLOY_TEMPLATE = "azuredeploy.json"
UI_TEMPLATE = "createUiDefinition.json"


dotenv_path = join(
    environ["HOME"],
    "dd",
    "azure-log-forwarding-orchestration",
    "scripts",
    "arm-template",
    ".env",
)

# generate tokens
if isfile(dotenv_path):
    with open(dotenv_path) as f:
        dotenv = {parts[0].strip(): parts[1].strip("\"' \t\n") for line in f if len(parts := line.split("=", 1)) == 2}
else:
    dotenv = {}

if not all(key in dotenv for key in ("deploy_token", "ui_token", "account", "rg")):
    print(".env missing fields, regenerating...")
    if "account" not in dotenv:
        dotenv["account"] = input("What is the storage account name?: ")
    if "rg" not in dotenv:
        dotenv["rg"] = input("What is the resource group name for the storage account?: ")

    if "deploy_token" not in dotenv or "ui_token" not in dotenv:
        duration = input("How long should the tokens be valid? [1 week]: ") or "1 week"
        connection = get_connection_string(dotenv["account"], dotenv["rg"])
        dotenv["deploy_token"] = get_token(DEPLOY_TEMPLATE, duration, connection)
        dotenv["ui_token"] = get_token(UI_TEMPLATE, duration, connection)
    print("writing to .env...", end="")
    with open(dotenv_path, "w") as f:
        f.write("\n".join(k + "=" + v for k, v in dotenv.items()))
    print("done")


url_base = "https://avargab74.blob.core.windows.net/templates/"


deploy_url = f"{url_base}{DEPLOY_TEMPLATE}?{dotenv['deploy_token']}"
ui_url = f"{url_base}{UI_TEMPLATE}?{dotenv['ui_token']}"

template_url = "https://portal.azure.com/#create/Microsoft.Template/uri/CustomDeploymentBlade/uri/{}/createUIDefinitionUri/{}".format(
    quote(deploy_url, safe=""), quote(ui_url, safe="")
)
print(template_url)
