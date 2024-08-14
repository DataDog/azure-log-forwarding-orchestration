package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"

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

type BlobContent struct {
	Name      string
	Container string
	Content   *[]byte
}

type BlobSegment struct {
	Name      string
	Container string
	Content   *[]byte
	Offset    int
	Count     int
}

func FromCurrentHour(blobName string) bool {
	isCurrentHour := strings.Contains(blobName, fmt.Sprintf("h=%02d", time.Now().UTC().Hour()))
	return FromToday(blobName) && isCurrentHour
}

// CheckBlobIsFromToday checks if the blob is from today given the current time.Now Day and Month
// parses the blob file string to check for
// EX: resourceId=/SUBSCRIPTIONS/xxx/RESOURCEGROUPS/xxx/PROVIDERS/MICROSOFT.WEB/SITES/xxx/y=2024/m=06/d=13/h=14/m=00/PT1H.json
func FromToday(blobName string) bool {
	isCurrentYear := strings.Contains(blobName, fmt.Sprintf("y=%02d", time.Now().Year()))
	isCurrentMonth := strings.Contains(blobName, fmt.Sprintf("m=%02d", time.Now().Month()))
	isCurrentDay := strings.Contains(blobName, fmt.Sprintf("d=%02d", time.Now().Day()))
	if isCurrentYear && isCurrentMonth && isCurrentDay {
		return true
	}
	return false
}

func FromCurrentMonth(blobName string) bool {
	isCurrentYear := strings.Contains(blobName, fmt.Sprintf("y=%02d", time.Now().Year()))
	isCurrentMonth := strings.Contains(blobName, fmt.Sprintf("m=%02d", time.Now().Month()))
	if isCurrentYear && isCurrentMonth {
		return true
	}
	return false
}

func getBlobItems(resp azblob.ListBlobsFlatResponse) []*container.BlobItem {
	if resp.Segment == nil {
		return nil
	}
	return resp.Segment.BlobItems
}

func (c *Client) DownloadRange(ctx context.Context, containerName string, blobName string, offset int, count int) (BlobSegment, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.DownloadBlob")
	defer span.Finish()

	options := &azblob.DownloadStreamOptions{
		Range: azblob.HTTPRange{Offset: int64(offset), Count: int64(count)},
	}

	resp, err := c.azBlobClient.DownloadStream(ctx, containerName, blobName, options)
	if err != nil {
		return BlobSegment{}, err
	}
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return BlobSegment{}, err
	}
	return BlobSegment{
		Name:      blobName,
		Container: containerName,
		Content:   &content,
		Offset:    offset,
		Count:     count,
	}, nil
}

func (c *Client) GetSize(ctx context.Context, containerName string, blobName string) (int, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.GetSize")
	defer span.Finish()
	blockClient, err := blockblob.NewClientFromConnectionString(c.connectionString, containerName, blobName, nil)
	if err != nil {
		return -1, err
	}

	props, err := blockClient.GetProperties(ctx, nil)
	if err != nil {
		return -1, err
	}
	return int(*props.ContentLength), nil
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
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.UploadBlob")
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
