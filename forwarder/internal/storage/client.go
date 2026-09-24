// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package storage

import (
	// stdlib
	"context"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/environment"
)

const managedIdentityCredential = "managedidentity"

// AzureBlobConfig contains the configuration required to create an Azure Blob Storage client.
type AzureBlobConfig struct {
	ConnectionString        string
	AccountName             string
	Credential              string
	ManagedIdentityClientID string
}

// AzureBlobConfigFromEnvironment loads Azure Blob Storage configuration from environment variables.
func AzureBlobConfigFromEnvironment() AzureBlobConfig {
	return AzureBlobConfig{
		ConnectionString:        environment.Get(environment.AzureWebJobsStorage),
		AccountName:             environment.Get(environment.AzureWebJobsStorageAccountName),
		Credential:              environment.Get(environment.AzureWebJobsStorageCredential),
		ManagedIdentityClientID: environment.Get(environment.AzureWebJobsStorageClientID),
	}
}

// NewAzureBlobClient creates an Azure Blob Storage client using managed identity or a connection string.

func NewAzureBlobClient(config AzureBlobConfig) (*azblob.Client, error) {
	if config.Credential == managedIdentityCredential {
		blobServiceURI := "https://" + config.AccountName + ".blob.core.windows.net"
		if config.ManagedIdentityClientID != "" {
			credential, _ := azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{
				ID: azidentity.ClientID(config.ManagedIdentityClientID),
			})
			return azblob.NewClient(blobServiceURI, credential, nil)
		}
		credential, _ := azidentity.NewDefaultAzureCredential(nil)
		return azblob.NewClient(blobServiceURI, credential, nil)
	}
	return azblob.NewClientFromConnectionString(config.ConnectionString, nil)
}

// AzureBlobClient wraps around the azblob.Client struct, to allow for mocking.
// these are the inherited and used methods.
//
//go:generate mockgen -package=mocks -source=$GOFILE -destination=mocks/mock_$GOFILE
type AzureBlobClient interface {
	NewListBlobsFlatPager(containerName string, o *azblob.ListBlobsFlatOptions) *runtime.Pager[azblob.ListBlobsFlatResponse]
	NewListContainersPager(o *azblob.ListContainersOptions) *runtime.Pager[azblob.ListContainersResponse]
	UploadBuffer(ctx context.Context, containerName string, blobName string, buffer []byte, o *azblob.UploadBufferOptions) (azblob.UploadBufferResponse, error)
	DownloadStream(ctx context.Context, containerName string, blobName string, o *azblob.DownloadStreamOptions) (azblob.DownloadStreamResponse, error)
	CreateContainer(ctx context.Context, containerName string, o *azblob.CreateContainerOptions) (azblob.CreateContainerResponse, error)
}

// Client is a storage client for Azure Blob Storage.
type Client struct {
	azBlobClient AzureBlobClient
}

// NewClient creates a new storage client.
func NewClient(azBlobClient AzureBlobClient) *Client {
	return &Client{
		azBlobClient: azBlobClient,
	}
}
