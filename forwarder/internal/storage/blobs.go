package storage

import (
	"context"
	"errors"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type Blob struct {
	Name      string
	Container string
}

func getBlobItems(resp azblob.ListBlobsFlatResponse) []*container.BlobItem {
	if resp.Segment == nil {
		return nil
	}
	return resp.Segment.BlobItems
}

func (c *Client) ListBlobs(ctx context.Context, containerName string) Iterator[[]*container.BlobItem, azblob.ListBlobsFlatResponse] {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.GetContainersMatchingPrefix")
	defer span.Finish()
	blobPager := c.azBlobClient.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
		Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
	})
	iter := NewIterator(blobPager, getBlobItems, nil)
	return iter
}

func (c *Client) UploadBlob(ctx context.Context, containerName string, blobName string, buffer []byte) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.UploadBuffer")
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
			_, err := c.azBlobClient.UploadBuffer(ctx, containerName, blobName, buffer, &uploadOptions)
			return err
		}
	}

	if downErr != nil {
		return downErr
	}

	downloadBuf, readErr := io.ReadAll(downloadResponse.Body)
	if readErr != nil {
		return readErr
	}

	downloadBuf = append(downloadBuf, "\n"...)
	downloadBuf = append(downloadBuf, buffer...)

	_, err := c.azBlobClient.UploadBuffer(ctx, containerName, blobName, downloadBuf, &uploadOptions)
	return err
}
