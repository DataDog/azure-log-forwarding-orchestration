package storage

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type Client struct {
	azBlobClient AzureBlobClient
}

func NewClient(storageAccountConnectionString string, options *azblob.ClientOptions) (*Client, error) {
	azBlobClient, err := azblob.NewClientFromConnectionString(storageAccountConnectionString, options)
	if err != nil {
		return nil, err
	}

	return &Client{
		azBlobClient: azBlobClient,
	}, nil
}

func NewClientWithAzBlobClient(azBlobClient AzureBlobClient) *Client {
	return &Client{
		azBlobClient: azBlobClient,
	}
}

// AzureBlobClient wraps around the azblob.Client struct, to allow for mocking.
// these are the inherited and used methods.
//
//go:generate mockgen -package=mocks -source=$GOFILE -destination=mocks/mock_$GOFILE
type AzureBlobClient interface {
	NewListContainersPager(o *azblob.ListContainersOptions) *runtime.Pager[azblob.ListContainersResponse]
}
