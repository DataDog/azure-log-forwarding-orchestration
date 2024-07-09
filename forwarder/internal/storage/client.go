package storage

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// AzureBlobClient wraps around the azblob.Client struct, to allow for mocking.
// these are the inherited and used methods.
type AzureBlobClient interface {
	NewListContainersPager(o *azblob.ListContainersOptions) AzurePager[azblob.ListContainersResponse]
}

type AzurePager[T any] interface {
	NextPage(ctx context.Context) (T, error)
	More() bool
}
