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
AzureWebJobsStorage='DefaultEndpointsProtocol=https;AccountName=...' go run cmd/forwarder/forwarder.go
```

### Running Datadog Performaance Tests
Create an Ubuntu VM in Azure.
Install the Datadog agent on the VM.
Follow the wizard at https://app.datadoghq.com/account/settings/agent/latest?platform=ubuntu
(Make certain APM is enabled)

Turn on process collection
https://docs.datadoghq.com/infrastructure/process/?tab=linuxwindows

GOOS=linux GOARCH=amd64 go build cmd/forwarder.go
scp forwarder azureuser@<VM IP ADDRESS>:/home/azureuser/forwarder


export AzureWebJobsStorage="<VALID CONNECTION STRING>"
/home/azureuser/forwarder



### Publishing and Running
Publish Custom function app in azure to run the cmd/forwarder.go file in the function app:

Via the cli:
```bash
GOOS=linux GOARCH=amd64 go build cmd/forwarder.go
func azure functionapp publish function-app-name --custom
```
