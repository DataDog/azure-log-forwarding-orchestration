package blobStorage

import (
	"context"
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"io"
)

// Rerun if changes are made to the interface
//go:generate mockgen -source=$GOFILE -destination=./tests/mocks/$GOFILE -package=mocks

// AzureBlobClient wraps around the azure blob Client struct these are the inherited and used methods
type AzureBlobClient interface {
	UploadStream(ctx context.Context, containerName string, blobName string, body io.Reader, o *azblob.UploadStreamOptions) (azblob.UploadStreamResponse, error)
	DownloadStream(ctx context.Context, containerName string, blobName string, o *azblob.DownloadStreamOptions) (azblob.DownloadStreamResponse, error)

	NewListBlobsFlatPager(containerName string, o *azblob.ListBlobsFlatOptions) *runtime.Pager[azblob.ListBlobsFlatResponse]
	NewListContainersPager(o *azblob.ListContainersOptions) *runtime.Pager[azblob.ListContainersResponse]

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

type AzureClient struct {
	Client         *azblob.Client
	Context        context.Context
	contextCancel  context.CancelFunc
	StorageAccount string
}

func NewAzureBlobClient(context context.Context, cancel context.CancelFunc, storageAccount string) (error, *AzureClient) {
	url := fmt.Sprintf(azureBlobURL, storageAccount)

	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return errors.New("failed to create azure credential"), nil
	}

	client, err := azblob.NewClient(url, credential, nil)
	if err != nil {
		return errors.New("failed to create azure client"), nil
	}

	return err, &AzureClient{
		Context:        context,
		Client:         client,
		contextCancel:  cancel,
		StorageAccount: storageAccount,
	}
}
