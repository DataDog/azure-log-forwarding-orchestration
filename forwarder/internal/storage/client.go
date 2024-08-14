package storage

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"google.golang.org/api/iterator"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
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
}

type Client struct {
	connectionString string
	azBlobClient     AzureBlobClient
}

func NewClient(azBlobClient AzureBlobClient, connectionString string) Client {
	return Client{
		connectionString: connectionString,
		azBlobClient:     azBlobClient,
	}
}

type Iterator[ReturnType any, PagerType any] struct {
	pager    *runtime.Pager[PagerType]
	getter   func(PagerType) ReturnType
	nilValue ReturnType
}

func (i *Iterator[ReturnType, PagerType]) Next(ctx context.Context) (r ReturnType, err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Iterator.Next")
	defer span.Finish(tracer.WithError(err))
	if !i.pager.More() {
		return i.nilValue, iterator.Done
	}

	resp, err := i.pager.NextPage(ctx)
	if err != nil {
		return i.nilValue, err
	}

	return i.getter(resp), nil
}

func NewIterator[ReturnType any, PagerType any](pager *runtime.Pager[PagerType], getter func(PagerType) ReturnType, nilValue ReturnType) Iterator[ReturnType, PagerType] {
	return Iterator[ReturnType, PagerType]{pager: pager, getter: getter, nilValue: nilValue}
}
