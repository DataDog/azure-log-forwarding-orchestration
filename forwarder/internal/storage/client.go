package storage

import (
	// stdlib
	"context"
	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// AzureBlobClient wraps around the azblob.Client struct, to allow for mocking.
// these are the inherited and used methods.
//
//go:generate mockgen -package=mocks -source=$GOFILE -destination=mocks/mock_$GOFILE
type AzureBlobClient interface {
	NewListBlobsFlatPager(containerName string, o *azblob.ListBlobsFlatOptions) *runtime.Pager[azblob.ListBlobsFlatResponse]
	NewListContainersPager(o *azblob.ListContainersOptions) *runtime.Pager[azblob.ListContainersResponse]
	UploadBuffer(ctx context.Context, containerName string, blobName string, buffer []byte, o *azblob.UploadBufferOptions) (azblob.UploadBufferResponse, error)
	DownloadStream(ctx context.Context, containerName string, blobName string, o *azblob.DownloadStreamOptions) (azblob.DownloadStreamResponse, error)
	DownloadBuffer(ctx context.Context, containerName string, blobName string, buffer []byte, o *azblob.DownloadBufferOptions) (int64, error)
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
