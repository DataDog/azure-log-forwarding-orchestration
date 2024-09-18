package storage

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type BlobSegment struct {
	Name      string
	Container string
	Content   *[]byte
	Offset    int
}

func (c *Client) DownloadSegment(ctx context.Context, blob Blob, offset int) (BlobSegment, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.DownloadBlob")
	defer span.Finish()

	options := &azblob.DownloadBufferOptions{
		Range:     azblob.HTTPRange{Offset: int64(offset)},
		BlockSize: 1024 * 1024,
	}

	content := make([]byte, int(*blob.Item.Properties.ContentLength))

	_, err := c.azBlobClient.DownloadBuffer(ctx, blob.Container, *blob.Item.Name, content, options)
	if err != nil {
		return BlobSegment{}, fmt.Errorf("failed to download blob: %w", err)
	}
	return BlobSegment{
		Name:      *blob.Item.Name,
		Container: blob.Container,
		Content:   &content,
		Offset:    offset,
	}, nil
}
