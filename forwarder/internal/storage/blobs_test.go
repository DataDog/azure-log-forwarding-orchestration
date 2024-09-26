package storage_test

import (
	// stdlib
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/go-autorest/autorest/to"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/api/iterator"

	// datadog
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage/mocks"
)

func newBlobItem(name string) *container.BlobItem {
	return &container.BlobItem{
		Name: to.StringPtr(name),
	}
}

func getListBlobsFlatResponse(containers []*container.BlobItem) azblob.ListBlobsFlatResponse {
	if containers == nil || len(containers) == 0 {
		return azblob.ListBlobsFlatResponse{}
	}
	return azblob.ListBlobsFlatResponse{
		ListBlobsFlatSegmentResponse: container.ListBlobsFlatSegmentResponse{
			Segment: &container.BlobFlatListSegment{
				BlobItems: containers,
			},
		},
	}
}

func listBlobs(t *testing.T, ctx context.Context, containerName string, responses [][]*container.BlobItem, fetcherError error) ([]*container.BlobItem, error) {
	ctrl := gomock.NewController(t)

	handler := storage.NewPagingHandler[[]*container.BlobItem, azblob.ListBlobsFlatResponse](responses, fetcherError, getListBlobsFlatResponse)

	pager := runtime.NewPager[azblob.ListBlobsFlatResponse](handler)

	mockClient := mocks.NewMockAzureBlobClient(ctrl)
	mockClient.EXPECT().NewListBlobsFlatPager(containerName, gomock.Any()).Return(pager)

	client := storage.NewClient(mockClient)

	span, ctx := tracer.StartSpanFromContext(context.Background(), "blobs.test")
	defer span.Finish()

	it := client.ListBlobs(ctx, containerName)

	var results []*container.BlobItem
	for {
		blobList, err := it.Next(ctx)

		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, err
		}

		for _, blob := range blobList {
			if blob == nil {
				continue
			}
			results = append(results, blob)
		}

	}
	return results, nil
}

func TestListBlobs(t *testing.T) {
	t.Parallel()

	t.Run("returns names of blobs", func(t *testing.T) {
		t.Parallel() // GIVEN
		testString := "test"
		firstPage := []*container.BlobItem{
			newBlobItem(testString),
			newBlobItem(testString),
		}

		// WHEN
		results, err := listBlobs(t, context.Background(), storage.LogContainerPrefix, [][]*container.BlobItem{firstPage}, nil)

		// THEN
		assert.NoError(t, err)
		assert.Len(t, results, 2)
		assert.Equal(t, testString, *results[0].Name)
		assert.Equal(t, testString, *results[1].Name)
	})

	t.Run("returns empty array", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		blobs := [][]*container.BlobItem{}

		// WHEN
		results, err := listBlobs(t, context.Background(), storage.LogContainerPrefix, blobs, nil)

		// THEN
		assert.NoError(t, err)
		assert.Len(t, results, 0)
	})

	t.Run("error response", func(t *testing.T) {
		t.Parallel()
		// GIVEN

		testString := "test"
		fetcherError := errors.New(testString)
		blobs := [][]*container.BlobItem{}

		// WHEN
		got, err := listBlobs(t, context.Background(), storage.LogContainerPrefix, blobs, fetcherError)

		// THEN
		assert.Nil(t, got)
		assert.Error(t, err)
		assert.Contains(t, fmt.Sprintf("%v", err), testString)
	})

	t.Run("multiple pages", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testString := "test"
		firstPage := []*container.BlobItem{
			newBlobItem(testString),
		}
		secondPage := []*container.BlobItem{
			newBlobItem(testString),
		}
		pages := [][]*container.BlobItem{firstPage, secondPage}

		// WHEN
		results, err := listBlobs(t, context.Background(), storage.LogContainerPrefix, pages, nil)

		// THEN
		assert.NoError(t, err)
		assert.Len(t, results, 2)
		assert.Equal(t, testString, *results[0].Name)
		assert.Equal(t, testString, *results[1].Name)
	})
}

func TestAppendBlob(t *testing.T) {
	t.Parallel()

	t.Run("uploads a buffer", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		ctrl := gomock.NewController(t)

		mockClient := mocks.NewMockAzureBlobClient(ctrl)
		client := storage.NewClient(mockClient)

		containerName := "container"
		blobName := "blob"
		buffer := []byte("data")
		oldBuffer := []byte("shiny!")
		stringReader := strings.NewReader(string(oldBuffer))
		stringReadCloser := io.NopCloser(stringReader)
		downResp := azblob.DownloadStreamResponse{}
		downResp.Body = stringReadCloser
		expectedBuffer := append(oldBuffer, []byte("\n")...)
		expectedBuffer = append(expectedBuffer, buffer...)

		mockClient.EXPECT().DownloadStream(gomock.Any(), containerName, blobName, gomock.Any()).Return(downResp, nil)
		mockClient.EXPECT().CreateContainer(gomock.Any(), containerName, gomock.Any()).Return(azblob.CreateContainerResponse{}, nil)
		mockClient.EXPECT().UploadBuffer(gomock.Any(), containerName, blobName, expectedBuffer, gomock.Any()).Return(azblob.UploadBufferResponse{}, nil)

		// WHEN
		err := client.AppendBlob(context.Background(), containerName, blobName, buffer)

		// THEN
		assert.Nil(t, err)
	})

	t.Run("uploads a buffer when there is no previous buffer", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		ctrl := gomock.NewController(t)

		mockClient := mocks.NewMockAzureBlobClient(ctrl)
		client := storage.NewClient(mockClient)

		containerName := "container"
		blobName := "blob"
		buffer := []byte("data")
		downResp := azblob.DownloadStreamResponse{}
		resp := http.Response{
			Body:       io.NopCloser(bytes.NewBufferString("test")),
			StatusCode: 404,
		}
		downErr := runtime.NewResponseErrorWithErrorCode(&resp, "BlobNotFound")

		mockClient.EXPECT().DownloadStream(gomock.Any(), containerName, blobName, gomock.Any()).Return(downResp, downErr)
		mockClient.EXPECT().CreateContainer(gomock.Any(), containerName, gomock.Any()).Return(azblob.CreateContainerResponse{}, nil)
		mockClient.EXPECT().UploadBuffer(gomock.Any(), containerName, blobName, buffer, gomock.Any()).Return(azblob.UploadBufferResponse{}, nil)

		// WHEN
		err := client.AppendBlob(context.Background(), containerName, blobName, buffer)

		// THEN
		assert.Nil(t, err)
	})

	t.Run("does not upload a buffer when there is an non BlobNotFound error", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		ctrl := gomock.NewController(t)

		mockClient := mocks.NewMockAzureBlobClient(ctrl)
		client := storage.NewClient(mockClient)

		containerName := "container"
		blobName := "blob"
		buffer := []byte("data")
		downResp := azblob.DownloadStreamResponse{}
		resp := http.Response{
			Body: io.NopCloser(bytes.NewBufferString("test")),
		}
		downErr := runtime.NewResponseErrorWithErrorCode(&resp, "Invalid")
		mockClient.EXPECT().DownloadStream(gomock.Any(), containerName, blobName, gomock.Any()).Return(downResp, downErr)

		// WHEN
		err := client.AppendBlob(context.Background(), containerName, blobName, buffer)

		// THEN
		assert.Contains(t, err.Error(), downErr.Error())
	})
}

func getBlob(creationTime time.Time) storage.Blob {
	return storage.Blob{
		Container: "container",
		Item: &container.BlobItem{
			Name: to.StringPtr("blob"),
			Properties: &container.BlobProperties{
				CreationTime: &creationTime,
			},
		},
	}
}

func TestCurrent(t *testing.T) {
	t.Parallel()

	t.Run("now is current", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		currTime := time.Now()
		blob := getBlob(currTime)

		// WHEN
		current := storage.Current(blob, currTime)

		// THEN
		assert.True(t, current)
	})

	t.Run("an hour ago is current", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		currTime := time.Now()
		blob := getBlob(currTime.Add(-1 * time.Hour))

		// WHEN
		current := storage.Current(blob, currTime)

		// THEN
		assert.True(t, current)
	})

	t.Run("three hours ago is not current", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		currTime := time.Now()
		blob := getBlob(currTime.Add(-3 * time.Hour))

		// WHEN
		current := storage.Current(blob, currTime)

		// THEN
		assert.False(t, current)
	})
}
