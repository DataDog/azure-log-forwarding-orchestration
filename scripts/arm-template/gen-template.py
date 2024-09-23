#!/usr/bin/env python3

import json
from subprocess import run
from os import chdir, environ, mkdir, path

chdir(path.join(environ["HOME"], "dd", "azure-log-forwarding-orchestration", "deploy"))

BUILD_DIR = "build"
TEMPLATE_FILE = "azuredeploy.json"
TEMPLATE_PATH = path.join(BUILD_DIR, TEMPLATE_FILE)

# create the build directory
if not path.exists(BUILD_DIR):
    mkdir(BUILD_DIR)

# build the template
run(["az", "bicep", "build", "-f", "azuredeploy.bicep", "--outdir", BUILD_DIR])

# read the template
with open(TEMPLATE_PATH) as template:
    template_json = json.load(template)

# fix the template
# this is a workaround for a bug in bicep where the control plane subdeployment needs to depend on the parent deployment properly,
# which because this is a management group template, the deployment is not within a subscription, even though bicep assumes it is
for resource in template_json["resources"]:
    if resource["name"] == "controlPlaneResourceGroup":
        resource["dependsOn"] = [
            "[subscriptionResourceId('Microsoft.Resources/deployments', 'createControlPlaneResourceGroup')]"
        ]
        break

# write the template
with open(TEMPLATE_PATH, "w") as template:
    json.dump(template_json, template, indent=2)
