package blobStorage

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"io"
)

// Rerun if changes are made to the interface
//go:generate mockgen -source=$GOFILE -destination=./tests/mocks/$GOFILE -package=mocks

// AzureBlobClient wraps around the azblob.Client struct, to allow for mocking.
// these are the inherited and used methods.
type AzureBlobClient interface {
	UploadStream(ctx context.Context, containerName string, blobName string, body io.Reader, o *azblob.UploadStreamOptions) (azblob.UploadStreamResponse, error)
	DownloadStream(ctx context.Context, containerName string, blobName string, o *azblob.DownloadStreamOptions) (azblob.DownloadStreamResponse, error)

	NewListBlobsFlatPager(containerName string, o *azblob.ListBlobsFlatOptions) *runtime.Pager[azblob.ListBlobsFlatResponse]
	NewListContainersPager(o *azblob.ListContainersOptions) *runtime.Pager[azblob.ListContainersResponse]

	// CreateContainer NewListBlobsFlatPager(containerName string, o *azblob.ListBlobsFlatOptions) *runtime.Pager[azblob.ListBlobsFlatResponse]
	//NewListContainersPager(o *azblob.ListContainersOptions) *runtime.Pager[azblob.ListContainersResponse]

	CreateContainer(ctx context.Context, containerName string, o *azblob.CreateContainerOptions) (azblob.CreateContainerResponse, error)
	DeleteContainer(ctx context.Context, containerName string, o *azblob.DeleteContainerOptions) (azblob.DeleteContainerResponse, error)
	DeleteBlob(ctx context.Context, containerName string, blobName string, o *azblob.DeleteBlobOptions) (azblob.DeleteBlobResponse, error)
}

type AzurePager[T any] interface {
	NextPage(ctx context.Context) (T, error)
	More() bool
}
