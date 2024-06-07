package blobCache

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"golang.org/x/sync/errgroup"
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

type ErrGroup interface {
	Go(Runnable)
	Wait() error
	SetLimit(int)
}

type AzureClient struct {
	Client         *azblob.Client
	Context        context.Context
	group          *errgroup.Group
	StorageAccount string
}

func NewAzureBlobClient(context context.Context, storageAccount string) *AzureClient {
	url := fmt.Sprintf(azureBlobURL, storageAccount)

	credential, err := azidentity.NewDefaultAzureCredential(nil)
	handleError(err)

	client, err := azblob.NewClient(url, credential, nil)
	handleError(err)

	return &AzureClient{
		Context:        context,
		Client:         client,
		StorageAccount: storageAccount,
	}
}

func InitializeCursorCacheContainer() {
	az := NewAzureBlobClient(context.Background(), "ninateststorage")
	_, err := az.Client.CreateContainer(az.Context, cursorContainerName, nil)
	if err != nil {
		if e, ok := err.(*azcore.ResponseError); ok && e.StatusCode == 409 {
			fmt.Println(e.RawResponse)
		} else {
			handleError(err)
		}
	}
	// This will always reset the cursor to nill when ran.
	// Should only be run once or during sa hard reset of the cache
	response := az.UploadBlobCursor(nil)
	fmt.Println(response)
	az.TeardownCursorCache()
	az.DownloadBlobCursor()
}

type Runnable func() error

func (w *AzureClient) Go(runnable Runnable) {
	w.group.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {

				return
			}
		}()

		err = runnable()
		return
	})
}
