package main

import (
	"context"
	"testing"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"
)

func getStorageClient(t *testing.T) *storage.Client {
	ctrl := gomock.NewController(t)
	blobClient := mocks.NewMockAzureBlobClient(ctrl)
	client := storage.NewClient(blobClient)
	return client
}

func TestProcessContainerChannel(t *testing.T) {
	t.Parallel()

	t.Run("returns names of containers", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		channelSize := 100

		client := getStorageClient(t)
		blobCh := make(chan storage.Blob, channelSize)
		containerCh := make(chan string, channelSize)
		eg, ctx := errgroup.WithContext(context.Background())

		// WHEN
		eg.Go(func() error {
			return storage.GetBlobsPerContainer(ctx, client, blobCh, containerCh)
		})
		err := eg.Wait()

		// THEN
		assert.NoError(t, err)
	})
}
