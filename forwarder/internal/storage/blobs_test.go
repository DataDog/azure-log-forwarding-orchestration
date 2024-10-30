package storage_test

import (
	// stdlib
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/go-autorest/autorest/to"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/collections"
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

func listBlobs(t *testing.T, ctx context.Context, blobContainer storage.Container, responses [][]*container.BlobItem, fetcherError error) []storage.Blob {
	ctrl := gomock.NewController(t)

	handler := collections.NewPagingHandler[[]*container.BlobItem, azblob.ListBlobsFlatResponse](responses, fetcherError, getListBlobsFlatResponse)

	pager := runtime.NewPager[azblob.ListBlobsFlatResponse](handler)

	mockClient := mocks.NewMockAzureBlobClient(ctrl)
	mockClient.EXPECT().NewListBlobsFlatPager(blobContainer.Name, gomock.Any()).Return(pager)

	client := storage.NewClient(mockClient)

	var output []byte
	buffer := bytes.NewBuffer(output)
	logger := log.New()
	logger.SetOutput(buffer)

	var blobs []storage.Blob
	it := client.ListBlobs(ctx, blobContainer, log.NewEntry(logger))
	for item := range it {
		blobs = append(blobs, item)
	}
	return blobs
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
		results := listBlobs(t, context.Background(), storage.Container{Name: storage.LogContainerPrefix}, [][]*container.BlobItem{firstPage}, nil)

		// THEN
		assert.Len(t, results, 2)
		assert.Equal(t, testString, results[0].Name)
		assert.Equal(t, testString, results[1].Name)
	})

	t.Run("empty slice input results in empty slice output", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		blobs := [][]*container.BlobItem{}

		// WHEN
		results := listBlobs(t, context.Background(), storage.Container{Name: storage.LogContainerPrefix}, blobs, nil)

		// THEN
		assert.Len(t, results, 0)
	})

	t.Run("error response", func(t *testing.T) {
		t.Parallel()
		// GIVEN

		testString := "test"
		fetcherError := errors.New(testString)
		blobs := [][]*container.BlobItem{}

		// WHEN
		results := listBlobs(t, context.Background(), storage.Container{Name: storage.LogContainerPrefix}, blobs, fetcherError)

		// THEN
		assert.Len(t, results, 0)
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
		results := listBlobs(t, context.Background(), storage.Container{Name: storage.LogContainerPrefix}, pages, nil)

		// THEN
		assert.Len(t, results, 2)
		assert.Equal(t, testString, results[0].Name)
		assert.Equal(t, testString, results[1].Name)
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
		Container:    storage.Container{Name: "container"},
		Name:         "blob",
		CreationTime: creationTime,
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
		current := blob.IsCurrent(currTime)

		// THEN
		assert.True(t, current)
	})

	t.Run("an hour ago is current", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		currTime := time.Now()
		blob := getBlob(currTime.Add(-1 * time.Hour))

		// WHEN
		got := blob.IsCurrent(currTime)

		// THEN
		assert.True(t, got)
	})

	t.Run("three hours ago is not current", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		currTime := time.Now()
		blob := getBlob(currTime.Add(-3 * time.Hour))

		// WHEN
		current := blob.IsCurrent(currTime)

		// THEN
		assert.False(t, current)
	})
}
