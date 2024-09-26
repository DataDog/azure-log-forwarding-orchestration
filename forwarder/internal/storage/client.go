package storage

import (
	// stdlib
	"context"
	"errors"
	"fmt"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"google.golang.org/api/iterator"

	// datadog
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
	DownloadBuffer(ctx context.Context, containerName string, blobName string, buffer []byte, o *azblob.DownloadBufferOptions) (int64, error)
	CreateContainer(ctx context.Context, containerName string, o *azblob.CreateContainerOptions) (azblob.CreateContainerResponse, error)
}

// Client is a storage client for Azure Blob Storage
type Client struct {
	azBlobClient AzureBlobClient
}

// NewClient creates a new storage client
func NewClient(azBlobClient AzureBlobClient) *Client {
	return &Client{
		azBlobClient: azBlobClient,
	}
}

// Iterator is a generic iterator for paginated responses
type Iterator[ReturnType any, PagerType any] struct {
	pager    *runtime.Pager[PagerType]
	getter   func(PagerType) ReturnType
	nilValue ReturnType
}

// Next returns the next item in the iterator
func (i *Iterator[ReturnType, PagerType]) Next(ctx context.Context) (r ReturnType, err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Iterator.Next")
	defer span.Finish(tracer.WithError(err))
	if !i.pager.More() {
		return i.nilValue, iterator.Done
	}

	resp, err := i.pager.NextPage(ctx)
	if err != nil {
		return i.nilValue, fmt.Errorf("getting next page: %w", err)
	}

	return i.getter(resp), nil
}

// NewIterator creates a new iterator
func NewIterator[ReturnType any, PagerType any](pager *runtime.Pager[PagerType], getter func(PagerType) ReturnType, nilValue ReturnType) Iterator[ReturnType, PagerType] {
	return Iterator[ReturnType, PagerType]{pager: pager, getter: getter, nilValue: nilValue}
}

// PagingError is returned when more items are fetched than expected
var PagingError = errors.New("fetched more items than expected")

// NewPagingHandler creates a new paging handler
func NewPagingHandler[ContentType any, ResponseType any](items []ContentType, fetcherError error, getter func(ContentType) ResponseType) runtime.PagingHandler[ResponseType] {
	counter := 0
	return runtime.PagingHandler[ResponseType]{
		Fetcher: func(ctx context.Context, response *ResponseType) (ResponseType, error) {
			var containersResponse ResponseType
			if fetcherError != nil {
				return containersResponse, fetcherError
			}
			if len(items) == 0 {
				counter++
				return containersResponse, nil
			}
			if counter >= len(items) {
				return containersResponse, PagingError
			}
			containersResponse = getter(items[counter])
			counter++
			return containersResponse, nil
		},
		More: func(response ResponseType) bool {
			return counter < len(items)
		},
	}
}
