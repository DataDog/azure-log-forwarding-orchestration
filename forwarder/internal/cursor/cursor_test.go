package cursor_test

import (
	// stdlib
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/cursor"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage/mocks"
)

func TestLoadCursors(t *testing.T) {
	t.Parallel()

	t.Run("reads cursors from azure", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testKey := "test"
		testValue := int64(300)
		cursors := cursor.NewCursors(nil)
		cursors.SetCursor(testKey, testValue)
		data, err := cursors.Bytes()
		reader := io.NopCloser(bytes.NewReader(data))
		response := azblob.DownloadStreamResponse{
			DownloadResponse: blob.DownloadResponse{
				Body: reader,
			},
		}

		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockAzureBlobClient(ctrl)
		mockClient.EXPECT().DownloadStream(gomock.Any(), storage.ForwarderContainer, cursor.BlobName, nil).Return(response, nil)

		client := storage.NewClient(mockClient)

		var output []byte
		buffer := bytes.NewBuffer(output)
		logger := log.New()
		logger.SetOutput(buffer)

		// WHEN
		got, err := cursor.LoadCursors(context.Background(), client, log.NewEntry(logger))

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, got)
		gotValue, found := got.GetCursor(testKey)
		assert.True(t, found)
		assert.Equal(t, testValue, gotValue)
	})

	t.Run("gives new cursors when file cannot be found", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockAzureBlobClient(ctrl)
		resp := http.Response{
			Body:       io.NopCloser(bytes.NewBufferString("test")),
			StatusCode: 404,
		}
		respErr := runtime.NewResponseErrorWithErrorCode(&resp, "BlobNotFound")
		mockClient.EXPECT().DownloadStream(gomock.Any(), storage.ForwarderContainer, cursor.BlobName, nil).Return(azblob.DownloadStreamResponse{}, respErr)

		client := storage.NewClient(mockClient)

		var output []byte
		buffer := bytes.NewBuffer(output)
		logger := log.New()
		logger.SetOutput(buffer)

		// WHEN
		got, err := cursor.LoadCursors(context.Background(), client, log.NewEntry(logger))

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, got)
	})
}

func TestSaveCursors(t *testing.T) {
	t.Parallel()

	t.Run("saves cursors to azure", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testKey := "test"
		testValue := int64(300)
		cursors := cursor.NewCursors(nil)
		cursors.SetCursor(testKey, testValue)
		response := azblob.UploadBufferResponse{}
		createContainerResponse := azblob.CreateContainerResponse{}

		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockAzureBlobClient(ctrl)
		mockClient.EXPECT().CreateContainer(gomock.Any(), storage.ForwarderContainer, nil).Return(createContainerResponse, nil)
		mockClient.EXPECT().UploadBuffer(gomock.Any(), storage.ForwarderContainer, cursor.BlobName, gomock.Any(), gomock.Any()).Return(response, nil)

		client := storage.NewClient(mockClient)

		// WHEN
		err := cursors.SaveCursors(context.Background(), client)

		// THEN
		assert.NoError(t, err)
	})
}
