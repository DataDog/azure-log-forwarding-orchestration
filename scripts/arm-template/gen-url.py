#!/usr/bin/env python3.11
"""Adapted from: https://learn.microsoft.com/en-us/azure/azure-resource-manager/templates/secure-template-with-sas-token?tabs=azure-cli#provide-sas-token-during-deployment"""

from urllib.parse import quote
from os.path import isfile, join
from os import environ
from subprocess import run


if (connection := environ.get("connection")) is None:
    raise ValueError("Please set the `connection` environment variable")
connection = connection.lower()
connection_parts = {
    kv[0]: kv[1] for kv in map(lambda seg: seg.split("="), connection.split(";"))
}


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
        dotenv = {
            parts[0].strip(): parts[1].strip("\"' \t\n")
            for line in f
            if len((parts := line.split("=", 1))) == 2
        }
else:
    dotenv = {}

if not all(key in dotenv for key in ("deploy_token", "ui_token")):
    print(".env missing fields, regenerating...")
    if "deploy_token" not in dotenv or "ui_token" not in dotenv:
        duration = input("How long should the tokens be valid? [1 week]: ") or "1 week"
        dotenv["deploy_token"] = get_token(DEPLOY_TEMPLATE, duration, connection)
        dotenv["ui_token"] = get_token(UI_TEMPLATE, duration, connection)
    print("writing to .env...", end="")
    with open(dotenv_path, "w") as f:
        f.write("\n".join(k + "=" + v for k, v in dotenv.items()))
    print("done")


url_base = f"{connection_parts['blobendpoint']}templates/"


deploy_url = f"{url_base}{DEPLOY_TEMPLATE}?{dotenv['deploy_token']}"
ui_url = f"{url_base}{UI_TEMPLATE}?{dotenv['ui_token']}"

template_url = "https://portal.azure.com/#create/Microsoft.Template/uri/CustomDeploymentBlade/uri/{}/createUIDefinitionUri/{}".format(
    quote(deploy_url, safe=""), quote(ui_url, safe="")
)
print(template_url)
