# Virtual Environment Setup

## Project Virtual Environment
The project has a Python virtual environment configured at `./venv`

### Python Binary Paths
- Python: `./venv/bin/python` or `./venv/bin/python3` (Python 3.14)
- Pip: `./venv/bin/pip` or `./venv/bin/pip3`

### Activation
```bash
source ./venv/bin/activate
```

### Required Packages (Already Installed)
- azure-identity (1.25.1)
- azure-mgmt-compute (37.2.0)
- azure-mgmt-network (30.1.0)
- azure-mgmt-resource (24.0.0)
- azure-mgmt-storage (24.0.0)
- azure-storage-blob (12.28.0)

### Running Deployment Scripts
Always use the venv Python:
```bash
./venv/bin/python scripts/deploy_personal_forwarder_vm.py
```

Or activate the venv first:
```bash
source ./venv/bin/activate
python scripts/deploy_personal_forwarder_vm.py
```

### Checking Package Status
```bash
./venv/bin/pip list | grep azure
```
