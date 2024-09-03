package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"google.golang.org/api/iterator"

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
	if err != nil {
		return nil, fmt.Errorf("failed to download blob %s: %w", blobName, err)
	}
	buffer, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("error reading existing blob %s: %v", blobName, readErr)
	}

	return buffer, nil
}

func (c *Client) UploadBlob(ctx context.Context, containerName string, blobName string, content []byte) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.UploadBlob")
	defer span.Finish()

	// create container if needed
	err := c.CreateContainer(ctx, containerName)
	if err != nil {
		return fmt.Errorf("error creating container %s: %v", containerName, err)
	}

	//see if there is an existing blob
	//if yes get it read append
	//if not just write

	buffer, downErr := c.DownloadBlob(ctx, containerName, blobName)
	//max-age = 2 hours or 7200 seconds
	cacheControlString := "max-age=7200"

	uploadOptions := azblob.UploadBufferOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobCacheControl: &cacheControlString,
		},
	}

	if downErr != nil && strings.Contains(downErr.Error(), "ERROR CODE: BlobNotFound") {
		// Create new file when not found
		_, err := c.azBlobClient.UploadBuffer(ctx, containerName, blobName, content, &uploadOptions)
		if err != nil {
			return fmt.Errorf("blob not found, failed to upload blob: %w", err)
		}
		return nil
	}

	if downErr != nil {
		return fmt.Errorf("error downloading existing blob %s: %v", blobName, downErr)
	}

	buffer = append(buffer, "\n"...)
	buffer = append(buffer, content...)

	_, err = c.azBlobClient.UploadBuffer(ctx, containerName, blobName, buffer, &uploadOptions)
	if err != nil {
		return fmt.Errorf("failed to upload blob %s: %w", blobName, err)
	}
	return nil
}

func getBlobs(ctx context.Context, client *Client, containerName string, blobChannel chan<- Blob) error {
	// Get the blobs from the container
	iter := client.ListBlobs(ctx, containerName)

	for {
		blobList, err := iter.Next(ctx)

		if errors.Is(err, iterator.Done) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("getting next page of blobs for %s: %v", containerName, err)
		}

		if blobList != nil {
			for _, b := range blobList {
				if b == nil {
					continue
				}
				blobChannel <- Blob{Item: b, Container: containerName}
			}
		}
	}

}

func GetBlobsPerContainer(ctx context.Context, client *Client, blobCh chan<- Blob, containerCh <-chan string) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.GetBlobsPerContainer")
	defer span.Finish()
	defer close(blobCh)
	var err error
	for c := range containerCh {
		curErr := getBlobs(ctx, client, c, blobCh)
		if curErr != nil {
			err = errors.Join(err, curErr)
		}
	}
	return err
}
