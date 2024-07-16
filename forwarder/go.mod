module github.com/DataDog/azure-log-forwarding-orchestration/forwarder

go 1.22.2

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.11.1
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.3.2
	github.com/Azure/go-autorest/autorest/to v0.4.0
	github.com/stretchr/testify v1.8.4
	go.uber.org/mock v0.4.0
	golang.org/x/sync v0.7.0
	google.golang.org/api v0.188.0
	gopkg.in/dnaeon/go-vcr.v3 v3.2.0
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.5.2 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.27.0 // indirect
	golang.org/x/text v0.16.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
