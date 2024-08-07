package storage

import (
	"context"

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
