package blobCache

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"io"
)

// Rerun if changes are made to the interface
//go:generate mockgen -source=$GOFILE -destination=./tests/mocks/$GOFILE -package=mocks

// GCPSecurityCenterClient wraps around the Google security center client struct
type AzureBlobClient interface {
	UploadStream(ctx context.Context, containerName string, blobName string, body io.Reader, o *azblob.UploadStreamOptions) (azblob.UploadStreamResponse, error)
	DownloadStream(ctx context.Context, containerName string, blobName string, o *azblob.DownloadStreamOptions) (azblob.DownloadStreamResponse, error)

	NewListBlobsFlatPager(containerName string, o *azblob.ListBlobsFlatOptions) *runtime.Pager[azblob.ListBlobsFlatResponse]
	NewListContainersPager(o *azblob.ListContainersOptions) *runtime.Pager[azblob.ListContainersResponse]

	CreateContainer(ctx context.Context, containerName string, o *azblob.CreateContainerOptions) (azblob.CreateContainerResponse, error)
	DeleteContainer(ctx context.Context, containerName string, o *azblob.DeleteContainerOptions) (azblob.DeleteContainerResponse, error)
	DeleteBlob(ctx context.Context, containerName string, blobName string, o *azblob.DeleteBlobOptions) (azblob.DeleteBlobResponse, error)
}

type AzureClient struct {
	Client         *azblob.Client
	Context        context.Context
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

// BlobC is currently only used in testing
func (c *AzureClient) BlobC() {
	//containerName := "insights-logs-functionapplogs"
	c.InitializeCursorCacheContainer()
	//containers := c.getLogContainers(false)
	cursor := c.DownloadBlobCursor()
	fmt.Println(cursor)
}
