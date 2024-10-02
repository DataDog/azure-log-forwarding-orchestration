package storage

import (
	// stdlib
	"context"
	"errors"
	"fmt"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

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

// Client is a storage client for Azure Blob Storage.
type Client struct {
	azBlobClient AzureBlobClient
}

// NewClient creates a new storage client.
func NewClient(azBlobClient AzureBlobClient) *Client {
	return &Client{
		azBlobClient: azBlobClient,
	}
}

// Done is returned by Iterator's Next method when the iteration is
// complete; when there are no more items to return.
var Done = errors.New("no more items in iterator")

// Iterator is a generic iterator for paginated responses containing lists.
type Iterator[ReturnType any, PagerType any] struct {
	azurePager    *runtime.Pager[PagerType]
	internalPager Pager[ReturnType]
	getter        func(PagerType) []ReturnType
	nilValue      ReturnType
}

type Pager[ReturnType any] struct {
	page      []ReturnType
	nextIndex int
}

func (p *Pager[ReturnType]) More() bool {
	return p.nextIndex < len(p.page)
}

func (p *Pager[ReturnType]) Next() ReturnType {
	currItem := p.page[p.nextIndex]
	p.nextIndex += 1
	return currItem
}

func NewPager[ReturnType any](page []ReturnType) Pager[ReturnType] {
	return Pager[ReturnType]{
		page: page,
	}
}

// Next returns the next item in the iterator.
func (i *Iterator[ReturnType, PagerType]) Next(ctx context.Context) (r ReturnType, err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Iterator.Next")
	defer span.Finish(tracer.WithError(err))
	if !i.internalPager.More() {
		err = i.getNextPage(ctx)
		if err != nil {
			return i.nilValue, err
		}
	}

	return i.internalPager.Next(), nil
}

func (i *Iterator[ReturnType, PagerType]) getNextPage(ctx context.Context) error {
	if !i.azurePager.More() {
		return Done
	}

	resp, err := i.azurePager.NextPage(ctx)
	if err != nil {
		return fmt.Errorf("getting next page: %w", err)
	}

	page := i.getter(resp)
	i.internalPager = NewPager(page)
	return nil
}

// NewIterator creates a new iterator.
func NewIterator[ReturnType any, PagerType any](pager *runtime.Pager[PagerType], getter func(PagerType) []ReturnType, nilValue ReturnType) Iterator[ReturnType, PagerType] {
	return Iterator[ReturnType, PagerType]{azurePager: pager, getter: getter, nilValue: nilValue}
}

// PagingError is returned when more items are fetched than expected.
var PagingError = errors.New("fetched more items than expected")

// NewPagingHandler creates a new paging handler.
func NewPagingHandler[ContentType any, ResponseType any](items []ContentType, fetcherError error, getter func(ContentType) ResponseType) runtime.PagingHandler[ResponseType] {
	counter := 0
	return runtime.PagingHandler[ResponseType]{
		Fetcher: func(ctx context.Context, response *ResponseType) (ResponseType, error) {
			var currResponse ResponseType
			if fetcherError != nil {
				return currResponse, fetcherError
			}
			if len(items) == 0 {
				counter++
				return currResponse, nil
			}
			if counter >= len(items) {
				return currResponse, PagingError
			}
			currResponse = getter(items[counter])
			counter++
			return currResponse, nil
		},
		More: func(response ResponseType) bool {
			return counter < len(items)
		},
	}
}
