// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package storage

import (
	// stdlib
	"context"
	"fmt"
	"strings"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
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
	BlobServiceURI          string
	Credential              string
	ManagedIdentityClientID string
	FederatedTokenFile      string
}

// AzureBlobConfigFromEnvironment loads Azure Blob Storage configuration from environment variables.
func AzureBlobConfigFromEnvironment() AzureBlobConfig {
	return AzureBlobConfig{
		ConnectionString:        environment.Get(environment.AzureWebJobsStorage),
		AccountName:             environment.Get(environment.AzureWebJobsStorageAccountName),
		BlobServiceURI:          environment.Get(environment.AzureWebJobsStorageBlobServiceURI),
		Credential:              environment.Get(environment.AzureWebJobsStorageCredential),
		ManagedIdentityClientID: environment.Get(environment.AzureWebJobsStorageClientID),
		FederatedTokenFile:      environment.Get(environment.AzureFederatedTokenFile),
	}
}

// NewAzureBlobClient creates an Azure Blob Storage client using managed identity, workload identity or a connection string.
func NewAzureBlobClient(config AzureBlobConfig) (*azblob.Client, error) {
	if strings.EqualFold(config.Credential, managedIdentityCredential) {
		blobServiceURI := strings.TrimSpace(config.BlobServiceURI)
		if blobServiceURI == "" {
			accountName := strings.TrimSpace(config.AccountName)
			if accountName == "" {
				return nil, fmt.Errorf("AzureWebJobsStorage__accountName or AzureWebJobsStorage__blobServiceUri must be set when using managed identity")
			}
			blobServiceURI = "https://" + accountName + ".blob.core.windows.net"
		}

		credential, err := newTokenCredential(config)
		if err != nil {
			return nil, err
		}
		return azblob.NewClient(blobServiceURI, credential, nil)
	}
	return azblob.NewClientFromConnectionString(config.ConnectionString, nil)
}

// newTokenCredential uses workload identity when a federated token file is set, as on AKS with workload identity, and managed identity otherwise.
func newTokenCredential(config AzureBlobConfig) (azcore.TokenCredential, error) {
	if config.FederatedTokenFile != "" {
		// An empty client ID falls back to AZURE_CLIENT_ID; the tenant ID always comes from AZURE_TENANT_ID.
		return azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
			ClientID:      config.ManagedIdentityClientID,
			TokenFilePath: config.FederatedTokenFile,
		})
	}

	var options *azidentity.ManagedIdentityCredentialOptions
	if config.ManagedIdentityClientID != "" {
		options = &azidentity.ManagedIdentityCredentialOptions{
			ID: azidentity.ClientID(config.ManagedIdentityClientID),
		}
	}
	return azidentity.NewManagedIdentityCredential(options)
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
