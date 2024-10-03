package storage_test

import (
	// stdlib
	"context"
	"errors"
	"testing"

	// 3p
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	// datadog
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage/mocks"
)

func downloadBlob(t *testing.T, ctx context.Context, containerName string, blobName string, buffer []byte, expectedResponse int64, expectedErr error) error {
	ctrl := gomock.NewController(t)

	mockClient := mocks.NewMockAzureBlobClient(ctrl)
	mockClient.EXPECT().DownloadBuffer(gomock.Any(), containerName, blobName, gomock.Any(), gomock.Any()).Return(expectedResponse, expectedErr)

	client := storage.NewClient(mockClient)

	span, ctx := tracer.StartSpanFromContext(context.Background(), "containers.test")
	defer span.Finish()

	blob := storage.Blob{
		Container:     containerName,
		Name:          blobName,
		ContentLength: int64(len(buffer)),
	}
	_, err := client.DownloadSegment(ctx, blob, 0)
	return err
}

func TestDownloadSegment(t *testing.T) {
	t.Parallel()

	t.Run("downloads a file successfully", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		containerName := "container"
		blobName := "blob"
		buffer := []byte("data")

		// WHEN
		err := downloadBlob(t, context.Background(), containerName, blobName, buffer, 0, nil)

		// THEN
		assert.Nil(t, err)
	})

	t.Run("downloading a file with an error has an error", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		containerName := "container"
		blobName := "blob"
		buffer := []byte("data")
		want := errors.New("someError")

		// WHEN
		got := downloadBlob(t, context.Background(), containerName, blobName, buffer, 0, want)

		// THEN
		assert.NotNil(t, got)
		assert.Contains(t, got.Error(), want.Error())
	})
}
