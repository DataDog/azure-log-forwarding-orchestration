package blobStorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
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

type Runnable func() error

type ErrGroup interface {
	Go(Runnable)
	Wait() error
	SetLimit(int)
}

type BlobClient struct {
	Client         *azblob.Client
	Context        context.Context
	ContextCancel  context.CancelFunc
	StorageAccount string
}

func NewBlobClient(context context.Context, cancel context.CancelFunc, storageAccount string) (*BlobClient, error) {

	client, err := azblob.NewClientFromConnectionString(storageAccount, nil)
	if err != nil {
		log.Println(err)
		fmt.Println(err)
		return nil, errors.New("failed to create azure client")
	}

	return &BlobClient{
		Context:        context,
		Client:         client,
		ContextCancel:  cancel,
		StorageAccount: storageAccount,
	}, nil

}

type AzurePager[T any] interface {
	NextPage(ctx context.Context) (T, error)
	More() bool
}
