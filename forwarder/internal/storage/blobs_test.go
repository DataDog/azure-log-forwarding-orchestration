package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/go-autorest/autorest/to"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/api/iterator"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
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

func uploadBlob(t *testing.T, ctx context.Context, containerName string, blobName string, buffer []byte, expectedUpResponse azblob.UploadBufferResponse, expectedUpErr error, expectedDownResponse azblob.DownloadStreamResponse, expectedDownErr error, upCalls int, downCalls int) error {
	ctrl := gomock.NewController(t)

	var newBuf []byte

	if expectedDownErr == nil {
		originalBuf, err := io.ReadAll(expectedDownResponse.Body)
		if err != nil {
			return err
		}

		//the body is empty after reading, thus we need to repopulate
		bodyString := string(originalBuf[:])
		stringReader := strings.NewReader(bodyString)
		stringReadCloser := io.NopCloser(stringReader)
		expectedDownResponse.Body = stringReadCloser

		newBuf = append(originalBuf, buffer...)
	} else {
		newBuf = buffer
	}

	mockClient := mocks.NewMockAzureBlobClient(ctrl)
	mockClient.EXPECT().CreateContainer(gomock.Any(), containerName, gomock.Any()).Return(azblob.CreateContainerResponse{}, nil)
	mockClient.EXPECT().UploadBuffer(gomock.Any(), containerName, blobName, newBuf, gomock.Any()).Return(expectedUpResponse, expectedUpErr).Times(upCalls)
	mockClient.EXPECT().DownloadStream(gomock.Any(), containerName, blobName, gomock.Any()).Return(expectedDownResponse, expectedDownErr).Times(downCalls)

	client := storage.NewClient(mockClient)

	span, ctx := tracer.StartSpanFromContext(context.Background(), "containers.test")
	defer span.Finish()

	return client.UploadBlob(ctx, containerName, blobName, buffer)
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

func TestUploadBlob(t *testing.T) {
	t.Parallel()

	t.Run("uploads a buffer", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		containerName := "container"
		blobName := "blob"
		buffer := []byte("data")
		expectedUpResponse := azblob.UploadBufferResponse{}
		stringReader := strings.NewReader("shiny!")
		stringReadCloser := io.NopCloser(stringReader)
		downResp := azblob.DownloadStreamResponse{}
		downResp.Body = stringReadCloser

		// WHEN
		err := uploadBlob(t, context.Background(), containerName, blobName, buffer, expectedUpResponse, nil, downResp, nil, 1, 1)

		// THEN
		assert.Nil(t, err)
	})

	t.Run("uploads a buffer when there is no previous buffer", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		containerName := "container"
		blobName := "blob"
		buffer := []byte("data")
		expectedUpResponse := azblob.UploadBufferResponse{}
		stringReader := strings.NewReader("shiny!")
		stringReadCloser := io.NopCloser(stringReader)
		downResp := azblob.DownloadStreamResponse{}
		downResp.Body = stringReadCloser
		resp := http.Response{
			Body: io.NopCloser(bytes.NewBufferString("test")),
		}
		downErr := runtime.NewResponseErrorWithErrorCode(&resp, "BlobNotFound")

		// WHEN
		err := uploadBlob(t, context.Background(), containerName, blobName, buffer, expectedUpResponse, nil, downResp, downErr, 1, 1)

		// THEN
		assert.Nil(t, err)
	})

	t.Run("uploads a buffer when there is an non BlobNotFound error", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		containerName := "container"
		blobName := "blob"
		buffer := []byte("data")
		expectedUpResponse := azblob.UploadBufferResponse{}
		stringReader := strings.NewReader("shiny!")
		stringReadCloser := io.NopCloser(stringReader)
		downResp := azblob.DownloadStreamResponse{}
		downResp.Body = stringReadCloser
		resp := http.Response{
			Body: io.NopCloser(bytes.NewBufferString("test")),
		}
		downErr := runtime.NewResponseErrorWithErrorCode(&resp, "Invalid")

		// WHEN
		err := uploadBlob(t, context.Background(), containerName, blobName, buffer, expectedUpResponse, nil, downResp, downErr, 0, 1)

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
