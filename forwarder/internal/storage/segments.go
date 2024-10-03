package storage

import (
	// stdlib
	"context"
	"fmt"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	// datadog
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

// BlobSegment represents a segment of a blob that could be partial or full
type BlobSegment struct {
	Name          string
	Container     string
	Content       []byte
	Offset        int64
	ContentLength int64
}

// DownloadSegment downloads a segment of a blob starting from an offset
func (c *Client) DownloadSegment(ctx context.Context, blob Blob, offset int64) (BlobSegment, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.DownloadBlob")
	defer span.Finish()

	options := &azblob.DownloadBufferOptions{
		Range:     azblob.HTTPRange{Offset: offset},
		BlockSize: 1024 * 1024,
	}

	content := make([]byte, int(blob.ContentLength))

	_, err := c.azBlobClient.DownloadBuffer(ctx, blob.Container, blob.Name, content, options)
	if err != nil {
		return BlobSegment{}, fmt.Errorf("failed to download blob: %w", err)
	}
	return BlobSegment{
		Name:          blob.Name,
		Container:     blob.Container,
		Content:       content,
		Offset:        offset,
		ContentLength: blob.ContentLength,
	}, nil
}
