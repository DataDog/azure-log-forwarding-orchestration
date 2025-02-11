# Arm Template

(this assumes your current directory is the root of the repo)

## Setup:

```bash
# ensure the az cli is installed
brew install azure-cli
# install the bicep cli
az bicep install
```

## Development:

To build the bicep into a valid ARM template, run `./ci/scripts/arm-template/build.py`

Then, the two components (createUiDefinition.json and azuredeploy.json) need to be uploaded to a storage bucket. Generate a connection string and run `scripts/arm-template/upload.sh`:
```bash
export connection=$(az storage account show-connection-string \
    --resource-group <storage acct rg> \
    --name <storage acct name> \
    --query connectionString)
$HOME/dd/azure-log-forwarding-orchestration/scripts/arm-template/upload.sh
```

Finally, generate a URL to test with using `scripts/arm-template/gen-url.py`.

You can reuse the URL for subsequent tests, since it will pull whatever was uploaded to the bucket it points to.
