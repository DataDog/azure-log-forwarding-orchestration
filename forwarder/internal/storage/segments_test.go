package storage_test

import (
	// stdlib
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage/mocks"
)

func TestDownloadSegment(t *testing.T) {
	t.Parallel()

	t.Run("downloads a file successfully", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		containerName := "container"
		blobName := "blob"
		want := "data"

		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockAzureBlobClient(ctrl)

		resp := azblob.DownloadStreamResponse{}
		resp.Body = io.NopCloser(strings.NewReader(want))

		mockClient.EXPECT().DownloadStream(gomock.Any(), containerName, blobName, gomock.Any()).Return(resp, nil)

		client := storage.NewClient(mockClient)

		blob := storage.Blob{
			Container:     containerName,
			Name:          blobName,
			ContentLength: int64(len(want)),
		}

		// WHEN
		segment, err := client.DownloadSegment(context.Background(), blob, 0)
		require.NoError(t, err)
		got := make([]byte, len(want))
		_, err = segment.Reader.Read(got)

		// THEN
		assert.Nil(t, err)
		assert.Equal(t, want, string(got))
	})

	t.Run("downloading a file with an error has an error", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		containerName := "container"
		blobName := "blob"
		want := errors.New("someError")

		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockAzureBlobClient(ctrl)

		resp := azblob.DownloadStreamResponse{}

		mockClient.EXPECT().DownloadStream(gomock.Any(), containerName, blobName, gomock.Any()).Return(resp, want)

		client := storage.NewClient(mockClient)

		blob := storage.Blob{
			Container:     containerName,
			Name:          blobName,
			ContentLength: 5,
		}

		// WHEN
		_, got := client.DownloadSegment(context.Background(), blob, 0)

		// THEN
		assert.NotNil(t, got)
		assert.Contains(t, got.Error(), want.Error())
	})
}
