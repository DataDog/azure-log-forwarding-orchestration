package storage

import (
	"context"
	"errors"
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

type BlobSegment struct {
	Name      string
	Container string
	Content   *[]byte
	Offset    int
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

func (c *Client) DownloadRange(ctx context.Context, blob Blob, offset int) (BlobSegment, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.DownloadBlob")
	defer span.Finish()

	options := &azblob.DownloadBufferOptions{
		Range:     azblob.HTTPRange{Offset: int64(offset)},
		BlockSize: 1024 * 1024,
	}

	content := make([]byte, int(*blob.Item.Properties.ContentLength)*1024)

	_, err := c.azBlobClient.DownloadBuffer(ctx, blob.Container, *blob.Item.Name, content, options)
	if err != nil {
		return BlobSegment{}, err
	}
	return BlobSegment{
		Name:      *blob.Item.Name,
		Container: blob.Container,
		Content:   &content,
		Offset:    offset,
	}, nil
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

func (c *Client) UploadBlob(ctx context.Context, containerName string, blobName string, content []byte) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.UploadBlob")
	defer span.Finish()

	//see if there is an existing blob
	//if yes get it read append
	//if not just write

	downloadResponse, downErr := c.azBlobClient.DownloadStream(ctx, containerName, blobName, nil)
	//max-age = 2 hours or 7200 seconds
	cacheControlString := "max-age=7200"

	uploadOptions := azblob.UploadBufferOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobCacheControl: &cacheControlString,
		},
	}

	var respErr *azcore.ResponseError

	if errors.As(downErr, &respErr) {
		// Create new file when not found
		if respErr.ErrorCode == "BlobNotFound" {
			_, err := c.azBlobClient.UploadBuffer(ctx, containerName, blobName, content, &uploadOptions)
			return err
		}
	}

	if downErr != nil {
		return downErr
	}

	buffer, readErr := io.ReadAll(downloadResponse.Body)
	if readErr != nil {
		return readErr
	}

	buffer = append(buffer, "\n"...)
	buffer = append(buffer, content...)

	_, err := c.azBlobClient.UploadBuffer(ctx, containerName, blobName, buffer, &uploadOptions)
	return err
}
