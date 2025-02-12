#!/usr/bin/env python
# Builds the ARM template
# Run from LFO root folder

import os
import tomllib
from shutil import copytree, rmtree
from subprocess import run
from strip_hints import strip_string_to_string

INITIAL_RUN_FILE = "./control_plane/tasks/initial_run.py"
INITIAL_RUN_BUILD = "initial_run.py"
INITIAL_RUN_SCRIPT = "initial_run.sh"
ARM_TEMPLATE_FILE = "./deploy/azuredeploy.bicep"
ARM_TEMPLATE_BUILD = "azuredeploy.json"


def read(file: str) -> str:
    with open(file) as f:
        return f.read()


def write(file: str, content: str) -> None:
    with open(file, "w") as f:
        f.write(content)


if os.path.isdir("./build"):
    rmtree("./build")

os.makedirs("./build", exist_ok=True)

# make a copy
copytree("./control_plane", "./build/control_plane")
os.chdir("./build")

for root, _, files in os.walk("."):
    for file in files:
        if file.endswith(".py"):
            path = os.path.join(root, file)
            content = read(path)
            content = content.replace("from typing import", "from typing_extensions import")
            content: str = strip_string_to_string(content, to_empty=True, strip_nl=True, no_ast=True)  # type: ignore
            write(path, content)


# ========================= INITIAL RUN BUILD =========================
print("Building initial run python script")
run(["stickytape", INITIAL_RUN_FILE, "--add-python-path", "./control_plane", "--output-file", INITIAL_RUN_BUILD])

project = tomllib.loads(read("./control_plane/pyproject.toml"))["project"]


deps: set[str] = set(project["dependencies"])
for task in ["resources_task", "diagnostic_settings_task", "scaling_task"]:
    deps.update(project["optional-dependencies"][task])


python_content = read(INITIAL_RUN_BUILD).replace("'", "'\"'\"'")  # Escape single quotes

script_content = f"""#!/usr/bin/env bash
# Bash script intended to be run on the azure-cli:2.65.0 image
set -euo pipefail
curl https://bootstrap.pypa.io/get-pip.py | python3
pip install {" ".join(deps)}
python3 -c '{python_content}'
"""

write(INITIAL_RUN_SCRIPT, script_content)
os.chdir("..")

# ========================= ARM TEMPLATE BUILD =========================
print("Building ARM template")
run(["az", "bicep", "build", "--file", ARM_TEMPLATE_FILE, "--outfile", ARM_TEMPLATE_BUILD])

print("ARM template built successfully and written to", ARM_TEMPLATE_BUILD)
