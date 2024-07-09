package storage

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// AzureBlobClient wraps around the azblob.Client struct, to allow for mocking.
// these are the inherited and used methods.
type AzureBlobClient interface {
	NewListContainersPager(o *azblob.ListContainersOptions) *runtime.Pager[azblob.ListContainersResponse]
}
