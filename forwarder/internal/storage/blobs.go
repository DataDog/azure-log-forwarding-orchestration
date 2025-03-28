package storage

import (
	// stdlib
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/collections"
)

// LookBackPeriod defines the period of time from now that we would look back for logs
const LookBackPeriod = -2 * time.Hour

// idBeginIndex is the index where the id begins after resourceId= in the blob name.
const idBeginIndex = 11

// idEndOffset is the offset from the end of the blob name to the end of the id.
// this is the length of the string /y=2024/m=10/d=28/h=16/m=00/PT1H.json
const idEndOffset = 37

// ErrInvalidResourceId is an error for when a blob name is invalid.
var ErrInvalidResourceId = errors.New("blob name does not include resource id")

// Blob represents a blob in a container.
type Blob struct {
	Container     Container
	Name          string
	ContentLength int64
	CreationTime  time.Time
}

// IsCurrent returns true if the blob was created within the look back period
func (b *Blob) IsCurrent(now time.Time) bool {
	return b.CreationTime.After(now.Add(LookBackPeriod))
}

// IsJson returns true if the blob is a json file.
func (b *Blob) IsJson() bool {
	return strings.HasSuffix(b.Name, ".json")
}

func (b *Blob) ResourceId() string {
	if len(b.Name) < idBeginIndex+idEndOffset {
		return ""
	}
	return b.Name[idBeginIndex : len(b.Name)-idEndOffset]
}

func NewBlob(container Container, item *container.BlobItem) Blob {
	newBlob := Blob{
		Container: container,
	}
	if item.Name != nil {
		newBlob.Name = *item.Name
	}

	if item.Properties != nil {
		if item.Properties.ContentLength != nil {
			newBlob.ContentLength = *item.Properties.ContentLength
		}
		if item.Properties.CreationTime != nil {
			newBlob.CreationTime = *item.Properties.CreationTime
		}
	}

	return newBlob
}

// ListBlobs returns an iterator over a sequence of blobs in a container.
func (c *Client) ListBlobs(ctx context.Context, storageContainer Container, logger *log.Entry) iter.Seq[Blob] {
	blobPager := c.azBlobClient.NewListBlobsFlatPager(storageContainer.Name, &azblob.ListBlobsFlatOptions{
		Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
	})

	return collections.New(ctx, blobPager, func(item azblob.ListBlobsFlatResponse) []Blob {
		if item.Segment == nil {
			return nil
		}
		return collections.FilterMap(item.Segment.BlobItems, func(blobItem *container.BlobItem) (Blob, bool) {
			return NewBlob(storageContainer, blobItem), blobItem != nil
		})
	}, logger)
}

// DownloadBlob downloads a blob from a container.
func (c *Client) DownloadBlob(ctx context.Context, containerName string, blobName string) ([]byte, error) {
	resp, err := c.azBlobClient.DownloadStream(ctx, containerName, blobName, nil)
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) && respErr.StatusCode == 404 {
		return nil, &NotFoundError{
			Item: blobName,
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to download blob %s: %w", blobName, err)
	}
	buffer, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("error reading existing blob %s: %w", blobName, readErr)
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

// UploadBlob uploads a blob to a container.
func (c *Client) UploadBlob(ctx context.Context, containerName string, blobName string, content []byte) error {
	// create container if needed
	err := c.CreateContainer(ctx, containerName)
	if err != nil {
		return fmt.Errorf("error creating container %s: %w", containerName, err)
	}

	_, err = c.azBlobClient.UploadBuffer(ctx, containerName, blobName, content, getUploadBufferOptions())
	if err != nil {
		return fmt.Errorf("failed to upload blob: %w", err)
	}
	return nil
}

// AppendBlob appends content to an existing blob in a container or creates a new one if it doesn't exist.
func (c *Client) AppendBlob(ctx context.Context, containerName string, blobName string, content []byte) error {
	//see if there is an existing blob
	//if yes get it read append
	//if not just write

	buffer, downErr := c.DownloadBlob(ctx, containerName, blobName)

	var notFoundErr *NotFoundError
	if errors.As(downErr, &notFoundErr) {
		return c.UploadBlob(ctx, containerName, blobName, content)
	}

	if downErr != nil {
		return fmt.Errorf("error downloading existing blob %s: %w", blobName, downErr)
	}

	buffer = append(buffer, content...)

	return c.UploadBlob(ctx, containerName, blobName, buffer)
}
