module github.com/DataDog/azure-log-forwarding-orchestration/forwarder

go 1.23.2
toolchain go1.24.1

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.14.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.3.2
	github.com/DataDog/datadog-api-client-go/v2 v2.29.0
	github.com/dop251/goja v0.0.0-20241024094426-79f3a7efcdbd
	github.com/google/uuid v1.6.0
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.9.0
	go.uber.org/mock v0.4.0
	golang.org/x/sync v0.11.0
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.10.0 // indirect
	github.com/DataDog/zstd v1.5.2 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	golang.org/x/net v0.36.0 // indirect
	golang.org/x/oauth2 v0.21.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
