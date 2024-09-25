package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

const MinusTwoHours = -2 * time.Hour

type Blob struct {
	Item      *container.BlobItem
	Container string
}

func Current(blob Blob, now time.Time) bool {
	return blob.Item.Properties.CreationTime.After(now.Add(MinusTwoHours))
}

func getBlobItems(resp azblob.ListBlobsFlatResponse) []*container.BlobItem {
	if resp.Segment == nil {
		return nil
	}
	return resp.Segment.BlobItems
}

func (c *Client) ListBlobs(ctx context.Context, containerName string) Iterator[[]*container.BlobItem, azblob.ListBlobsFlatResponse] {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.GetContainersMatchingPrefix")
	defer span.Finish()
	blobPager := c.azBlobClient.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
		Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
	})
	iter := NewIterator(blobPager, getBlobItems, nil)
	return iter
}

func (c *Client) DownloadBlob(ctx context.Context, containerName string, blobName string) ([]byte, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.DownloadBlob")
	defer span.Finish()

	resp, err := c.azBlobClient.DownloadStream(ctx, containerName, blobName, nil)
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) && respErr.StatusCode == 404 {
		return nil, &NotFoundError{}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to download blob %s: %w", blobName, err)
	}
	buffer, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("error reading existing blob %s: %v", blobName, readErr)
	}

	return buffer, nil
}

func getUploadBufferOptions() *azblob.UploadBufferOptions {
	//max-age = 2 hours or 7200 seconds
	cacheControlString := "max-age=7200"
	return &azblob.UploadBufferOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobCacheControl: &cacheControlString,
		},
	}
}

func (c *Client) UploadBlob(ctx context.Context, containerName string, blobName string, content []byte) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.UploadBlob")
	defer span.Finish()

	// create container if needed
	err := c.CreateContainer(ctx, containerName)
	if err != nil {
		return fmt.Errorf("error creating container %s: %v", containerName, err)
	}

	_, err = c.azBlobClient.UploadBuffer(ctx, containerName, blobName, content, getUploadBufferOptions())
	if err != nil {
		return fmt.Errorf("failed to upload blob: %w", err)
	}
	return nil
}

func (c *Client) AppendBlob(ctx context.Context, containerName string, blobName string, content []byte) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.AppendBlob")
	defer span.Finish()

	//see if there is an existing blob
	//if yes get it read append
	//if not just write

	buffer, downErr := c.DownloadBlob(ctx, containerName, blobName)

	var notFoundErr *NotFoundError
	if errors.As(downErr, &notFoundErr) {
		return c.UploadBlob(ctx, containerName, blobName, content)
	}

	if downErr != nil {
		return fmt.Errorf("error downloading existing blob %s: %v", blobName, downErr)
	}

	buffer = append(buffer, "\n"...)
	buffer = append(buffer, content...)

	return c.UploadBlob(ctx, containerName, blobName, buffer)
}
