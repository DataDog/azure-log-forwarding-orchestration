## Getting Started

Make sure you have go installed
```
$ go version
go version go1.22.4 darwin/arm64
```

Install dependencies
```bash
go mod vendor
```

### Local Testing Environment

To test the blob forwarder locally, get the connection string for the storage account you want to test and add it as an environment variable.
An example is below:

```bash
AzureWebJobsStorage='DefaultEndpointsProtocol=https;AccountName=...' go run main.go
```

### Publishing and Running
Publish Custom function app in azure to run the main.go file in the function app:

Via the cli:
```bash
GOOS=linux GOARCH=amd64 go build main.go
func azure functionapp publish function-app-name --custom
```
