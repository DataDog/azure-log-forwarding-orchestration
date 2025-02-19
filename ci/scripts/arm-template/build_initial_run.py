#!/usr/bin/env python
# Builds the ARM template
# Run from LFO root folder

import os
import tomllib
from shutil import rmtree
from subprocess import run

INITIAL_RUN_FILE = "./control_plane/tasks/initial_run.py"
INITIAL_RUN_BUILD = "./build/initial_run.py"
INITIAL_RUN_SCRIPT = "./build/initial_run.sh"
ARM_TEMPLATE_FILE = "./deploy/azuredeploy.bicep"
ARM_TEMPLATE_BUILD = "./build/azuredeploy.json"

if os.path.isdir("./build"):
    rmtree("./build")

os.makedirs("./build", exist_ok=True)

# ========================= INITIAL RUN BUILD =========================
print("Building initial run python script")
run(["stickytape", INITIAL_RUN_FILE, "--add-python-path", "./control_plane", "--output-file", INITIAL_RUN_BUILD])

with open("./control_plane/pyproject.toml", "b") as f:
    project = tomllib.load(f)["project"]


deps: set[str] = set(project["dependencies"])
for task in ["resources_task", "diagnostic_settings_task", "scaling_task"]:
    deps.update(project["optional-dependencies"][task])

with open(INITIAL_RUN_BUILD) as f:
    python_content = f.read().replace("'", "'\"'\"'")  # Escape single quotes

script_content = f"""#!/usr/bin/env bash
# Bash script intended to be run on the azure-cli:2.65.0 image
set -euo pipefail
curl https://bootstrap.pypa.io/get-pip.py | python3
pip install {" ".join(deps)}
python3 -c '{python_content}'
"""

with open(INITIAL_RUN_SCRIPT, "w") as f:
    f.write(script_content)
print("Initial run script built and written to", INITIAL_RUN_SCRIPT)
