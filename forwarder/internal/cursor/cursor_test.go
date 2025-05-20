// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package cursor_test

import (
	// stdlib
	"bytes"
	"context"
	"errors"
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
		testContainerName := "test"
		testBlobName := "test"
		testValue := int64(300)
		cursors := cursor.New(nil)
		cursors.Set(testContainerName, testBlobName, testValue)
		data, err := cursors.JSONBytes()
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
		got, err := cursor.Load(context.Background(), client, log.NewEntry(logger))

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, got)
		gotValue := got.Get(testContainerName, testBlobName)
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
		got, err := cursor.Load(context.Background(), client, log.NewEntry(logger))

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
		testContainerName := "test"
		testBlobName := "test"
		testValue := int64(300)
		cursors := cursor.New(nil)
		cursors.Set(testContainerName, testBlobName, testValue)
		response := azblob.UploadBufferResponse{}
		createContainerResponse := azblob.CreateContainerResponse{}

		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockAzureBlobClient(ctrl)
		mockClient.EXPECT().CreateContainer(gomock.Any(), storage.ForwarderContainer, nil).Return(createContainerResponse, nil)
		mockClient.EXPECT().UploadBuffer(gomock.Any(), storage.ForwarderContainer, cursor.BlobName, gomock.Any(), gomock.Any()).Return(response, nil)

		client := storage.NewClient(mockClient)

		// WHEN
		err := cursors.Save(context.Background(), client)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("saves cursors to azure even if a timeout already occurred in the forwarder", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testContainerName := "test"
		testBlobName := "test"
		testValue := int64(300)
		cursors := cursor.New(nil)
		cursors.Set(testContainerName, testBlobName, testValue)
		response := azblob.UploadBufferResponse{}
		createContainerResponse := azblob.CreateContainerResponse{}

		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockAzureBlobClient(ctrl)
		mockClient.EXPECT().CreateContainer(gomock.Any(), storage.ForwarderContainer, nil).Return(createContainerResponse, nil)
		mockClient.EXPECT().UploadBuffer(gomock.Any(), storage.ForwarderContainer, cursor.BlobName, gomock.Any(), gomock.Any()).Return(response, nil)

		client := storage.NewClient(mockClient)

		timedOutCtx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()

		// WHEN
		err := cursors.Save(timedOutCtx, client)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("Save() returns an error if UploadBuffer() fails", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testContainerName := "test"
		testBlobName := "test"
		testValue := int64(300)

		cursors := cursor.New(nil)
		cursors.Set(testContainerName, testBlobName, testValue)
		response := azblob.UploadBufferResponse{}
		createContainerResponse := azblob.CreateContainerResponse{}

		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockAzureBlobClient(ctrl)
		errorMessage := "test error - blob upload failed"
		mockClient.EXPECT().CreateContainer(gomock.Any(), storage.ForwarderContainer, nil).Return(createContainerResponse, nil)
		mockClient.EXPECT().UploadBuffer(gomock.Any(), storage.ForwarderContainer, cursor.BlobName, gomock.Any(), gomock.Any()).Return(response, errors.New(errorMessage))

		client := storage.NewClient(mockClient)

		// WHEN
		err := cursors.Save(context.Background(), client)

		// THEN
		assert.Error(t, err)
		assert.ErrorContains(t, err, errorMessage)
	})

	t.Run("Save() returns an error if CreateContainer() fails", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testContainerName := "test"
		testBlobName := "test"
		testValue := int64(300)

		cursors := cursor.New(nil)
		cursors.Set(testContainerName, testBlobName, testValue)
		createContainerResponse := azblob.CreateContainerResponse{}

		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockAzureBlobClient(ctrl)
		errorMessage := "test error - container creation failed"
		mockClient.EXPECT().CreateContainer(gomock.Any(), storage.ForwarderContainer, nil).Return(createContainerResponse, errors.New(errorMessage))

		client := storage.NewClient(mockClient)

		// WHEN
		err := cursors.Save(context.Background(), client)

		// THEN
		assert.Error(t, err)
		assert.ErrorContains(t, err, errorMessage)
	})
}
