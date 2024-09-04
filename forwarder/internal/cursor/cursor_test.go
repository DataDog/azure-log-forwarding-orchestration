package cursor_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/cursor"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestLoadCursors(t *testing.T) {
	t.Parallel()

	t.Run("reads cursors from azure", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testKey := "test"
		testValue := 300
		cursors := cursor.NewCursors(nil)
		cursors.SetCursor(testKey, testValue)
		data, err := cursors.GetRawCursors()
		reader := io.NopCloser(bytes.NewReader(data))
		response := azblob.DownloadStreamResponse{
			DownloadResponse: blob.DownloadResponse{
				Body: reader,
			},
		}

		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockAzureBlobClient(ctrl)
		mockClient.EXPECT().DownloadStream(gomock.Any(), cursor.CursorContainer, cursor.CursorBlob, nil).Return(response, nil)

		client := storage.NewClient(mockClient)

		// WHEN
		got, err := cursor.LoadCursors(context.Background(), client)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, got)
		gotValue, err := got.GetCursor(testKey)
		assert.NoError(t, err)
		assert.Equal(t, testValue, gotValue)
	})

	t.Run("gives new cursors when file cannot be found", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		response := azblob.DownloadStreamResponse{}
		respErr := errors.New("The specified container does not exist uh oh")

		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockAzureBlobClient(ctrl)
		mockClient.EXPECT().DownloadStream(gomock.Any(), cursor.CursorContainer, cursor.CursorBlob, nil).Return(response, respErr)

		client := storage.NewClient(mockClient)

		// WHEN
		got, err := cursor.LoadCursors(context.Background(), client)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, got)
		assert.Equal(t, 0, got.Length())
	})
}

func TestSaveCursors(t *testing.T) {
	t.Parallel()

	t.Run("saves cursors to azure", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testKey := "test"
		testValue := 300
		cursors := cursor.NewCursors(nil)
		cursors.SetCursor(testKey, testValue)
		response := azblob.UploadBufferResponse{}
		createContainerResponse := azblob.CreateContainerResponse{}

		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockAzureBlobClient(ctrl)
		mockClient.EXPECT().CreateContainer(gomock.Any(), cursor.CursorContainer, nil).Return(createContainerResponse, nil)
		mockClient.EXPECT().UploadBuffer(gomock.Any(), cursor.CursorContainer, cursor.CursorBlob, gomock.Any(), gomock.Any()).Return(response, nil)

		client := storage.NewClient(mockClient)

		// WHEN
		err := cursors.SaveCursors(context.Background(), client)

		// THEN
		assert.NoError(t, err)
	})
}
