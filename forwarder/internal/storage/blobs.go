package storage

import (
	"context"
	"errors"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
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
	var respErr *azcore.ResponseError

	downloadResponse, errDown := c.azBlobClient.DownloadStream(ctx, containerName, blobName, nil)

	if errors.As(errDown, &respErr) {
		// Handle Error
		if respErr.ErrorCode == "BlobNotFound" {
			_, err := c.azBlobClient.UploadBuffer(ctx, containerName, blobName, buffer, nil)
			return err
		} else {
			return errDown
		}
	}
	if errDown != nil {
		return errDown
	}

	orig_buf, read_err := io.ReadAll(downloadResponse.Body)
	if read_err != nil {
		return errDown
	}

	orig_buf = append(orig_buf, "\n"...)
	orig_buf = append(orig_buf, buffer...)

	_, err := c.azBlobClient.UploadBuffer(ctx, containerName, blobName, orig_buf, nil)
	return err
}
