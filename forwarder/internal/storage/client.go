package storage

import (
	"context"
	"errors"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
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
	azBlobClient AzureBlobClient
}

func NewClient(azBlobClient AzureBlobClient) Client {
	return Client{
		azBlobClient: azBlobClient,
	}
}

func (c *Client) UploadBuffer(ctx context.Context, containerName string, blobName string, buffer []byte) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.UploadBuffer")
	defer span.Finish()

	//see if there is an existing blob
	//if yes get it read append
	//if not just write
	var respErr *azcore.ResponseError

	downloadResponse, err_down := c.azBlobClient.DownloadStream(ctx, containerName, blobName, nil)

	if errors.As(err_down, &respErr) {
		// Handle Error
		if respErr.ErrorCode == "BlobNotFound" {
			_, err := c.azBlobClient.UploadBuffer(ctx, containerName, blobName, buffer, nil)
			return err
		} else {
			return err_down
		}
	}
	if err_down != nil {
		return err_down
	}

	orig_buf, read_err := io.ReadAll(downloadResponse.Body)
	if read_err != nil {
		return err_down
	}

	orig_buf = append(orig_buf, "\n"...)
	orig_buf = append(orig_buf, buffer...)

	_, err := c.azBlobClient.UploadBuffer(ctx, containerName, blobName, orig_buf, nil)
	return err
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
