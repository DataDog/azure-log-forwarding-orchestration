package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type BlobCursorMap interface {
	GetCursor(key string) (int, error)
	SetCursor(key string, offset int)
}

type BlobSegment struct {
	Name          string
	Container     string
	Content       *[]byte
	Offset        int64
	ContentLength int64
}

func (c *Client) DownloadSegment(ctx context.Context, blob Blob, offset int64) (BlobSegment, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.DownloadBlob")
	defer span.Finish()

	options := &azblob.DownloadBufferOptions{
		Range:     azblob.HTTPRange{Offset: offset},
		BlockSize: 1024 * 1024,
	}

	content := make([]byte, int(*blob.Item.Properties.ContentLength))

	_, err := c.azBlobClient.DownloadBuffer(ctx, blob.Container, *blob.Item.Name, content, options)
	if err != nil {
		return BlobSegment{}, fmt.Errorf("failed to download blob: %w", err)
	}
	return BlobSegment{
		Name:          *blob.Item.Name,
		Container:     blob.Container,
		Content:       &content,
		Offset:        offset,
		ContentLength: *blob.Item.Properties.ContentLength,
	}, nil
}

func getBlobContents(ctx context.Context, client *Client, blob Blob, blobContentChannel chan<- BlobSegment, cursors BlobCursorMap) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "forwarder.getBlobContents")
	defer span.Finish(tracer.WithError(err))

	currentOffset, _ := cursors.GetCursor(*blob.Item.Name)
	current, err := client.DownloadSegment(ctx, blob, int64(currentOffset))
	if err != nil {
		return fmt.Errorf("download range for %s: %v", *blob.Item.Name, err)
	}

	cursors.SetCursor(current.Name, int(current.ContentLength))
	blobContentChannel <- current
	return nil
}

func GetBlobContents(ctx context.Context, logger *log.Entry, client *Client, blobCh <-chan Blob, blobContentCh chan<- BlobSegment, now time.Time, cursors BlobCursorMap) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.GetBlobContents")
	defer span.Finish()
	defer close(blobContentCh)
	blobsEg, ctx := errgroup.WithContext(ctx)
	for blob := range blobCh {
		if !Current(blob, now) {
			continue
		}
		logger.Printf("Downloading blob %s", *blob.Item.Name)
		blobsEg.Go(func() error { return getBlobContents(ctx, client, blob, blobContentCh, cursors) })
	}
	return blobsEg.Wait()
}
