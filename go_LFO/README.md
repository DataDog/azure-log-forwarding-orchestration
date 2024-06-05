## Getting Started

### One time setup

Install the Core Tools package:
```bash
brew update && brew install azure-cli
brew tap azure/functions
brew install azure-functions-core-tools@4
go mod vendor
```

Set up Azure Environment:
```bash
az upgrade
az login
go get github.com/Azure/azure-sdk-for-go/sdk/storage/azblob
go get github.com/Azure/azure-sdk-for-go/sdk/azidentity
go mod tidy
```
assign `Storage Blob Data Contributor` to the Storage Account:

In your storage account, select Access Control (IAM). Click Add and select add role assignment

Set up Datadog Environment:
```bash
pre-commit install
go get github.com/golang/mock/gomock
go mod vedor
```

### Create Blob Storage Container

must run the Blob Storage Container creation script before running the function app:
this can be found inside the initializeBlobCache.go file

### Publishing and Running
Publish Custom function app in azure to run the main.go file in the function app:

Via the cli:
```bash
func azure functionapp publish function-app-name --custom
```
