## Getting Started

### One time setup

Install the Core Tools package:
```bash
brew update && brew install azure-cli
brew tap azure/functions
brew install azure-functions-core-tools@4
go mod vendor
```

### Set up Azure Environment:
#### Assign `Storage Blob Data Contributor` to the Storage Account:

In your storage account, select Access Control (IAM). Click Add and select add role assignment
Select the role `Storage Blob Data Contributor`

#### Inside you terminal
```bash
az upgrade
az login
```

### Install Required Go packages:
```bash
az upgrade
az login
go get github.com/Azure/azure-sdk-for-go/sdk/storage/azblob
go get github.com/Azure/azure-sdk-for-go/sdk/azidentity
go mod tidy
go get golang.org/x/sync/errgroup
go get github.com/golang/mock/gomock
go mod vedor
```

### Publishing and Running
Publish Custom function app in azure to run the main.go file in the function app:

Via the cli:
```bash
func azure functionapp publish function-app-name --custom
```
