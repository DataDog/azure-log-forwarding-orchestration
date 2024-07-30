package storage_test

import (
	"context"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func newPagingHandler[ContentType any, ResponseType any](items []ContentType, fetcherError error, getter func(ContentType) ResponseType, nilValue ResponseType) runtime.PagingHandler[ResponseType] {
	counter := 0
	return runtime.PagingHandler[ResponseType]{
		Fetcher: func(ctx context.Context, response *ResponseType) (ResponseType, error) {
			var containersResponse ResponseType
			if fetcherError != nil {
				return nilValue, fetcherError
			}
			if len(items) == 0 {
				counter++
				return nilValue, nil
			}
			containersResponse = getter(items[counter])
			counter++
			return containersResponse, nil
		},
		More: func(response ResponseType) bool {
			return counter < len(items)
		},
	}
}

func uploadBuffer(t *testing.T, ctx context.Context, containerName string, blobName string, buffer []byte, expectedResponse azblob.UploadBufferResponse, expectedErr error) error {
	ctrl := gomock.NewController(t)

	mockClient := mocks.NewMockAzureBlobClient(ctrl)
	mockClient.EXPECT().UploadBuffer(gomock.Any(), containerName, blobName, buffer, gomock.Any()).Return(expectedResponse, expectedErr)

	client := storage.NewClient(mockClient)

	span, ctx := tracer.StartSpanFromContext(context.Background(), "containers.test")
	defer span.Finish()

	return client.UploadBuffer(ctx, containerName, blobName, buffer)
}

func TestUploadBuffer(t *testing.T) {
	t.Parallel()

	t.Run("uploads a buffer", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		containerName := "container"
		blobName := "blob"
		buffer := []byte("data")
		expectedResponse := azblob.UploadBufferResponse{}

		// WHEN
		err := uploadBuffer(t, context.Background(), containerName, blobName, buffer, expectedResponse, nil)

		// THEN
		assert.Nil(t, err)
	})
}
