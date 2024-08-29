package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/go-autorest/autorest/to"
	log "github.com/sirupsen/logrus"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func newContainerItem(name string) *service.ContainerItem {
	return &service.ContainerItem{
		Name: to.StringPtr(name),
	}
}

func getListContainersResponse(containers []*service.ContainerItem) azblob.ListContainersResponse {
	return azblob.ListContainersResponse{
		ListContainersSegmentResponse: service.ListContainersSegmentResponse{
			ContainerItems: containers,
		},
	}
}

func newBlobItem(name string) *container.BlobItem {
	now := time.Now()
	return &container.BlobItem{
		Name: to.StringPtr(name),
		Properties: &container.BlobProperties{
			ContentLength: to.Int64Ptr(1024),
			CreationTime:  &now,
		},
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

func TestRun(t *testing.T) {
	t.Parallel()

	t.Run("execute the basic functionality", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testString := "test"
		containerPage := []*service.ContainerItem{
			newContainerItem(testString),
			newContainerItem(testString),
		}
		blobPage := []*container.BlobItem{
			newBlobItem(testString),
			newBlobItem(testString),
		}

		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockAzureBlobClient(ctrl)

		containerHandler := storage.NewPagingHandler[[]*service.ContainerItem, azblob.ListContainersResponse]([][]*service.ContainerItem{containerPage}, nil, getListContainersResponse)
		containerPager := runtime.NewPager[azblob.ListContainersResponse](containerHandler)
		mockClient.EXPECT().NewListContainersPager(gomock.Any()).Return(containerPager)

		blobHandler := storage.NewPagingHandler[[]*container.BlobItem, azblob.ListBlobsFlatResponse]([][]*container.BlobItem{blobPage}, nil, getListBlobsFlatResponse)
		blobPager := runtime.NewPager[azblob.ListBlobsFlatResponse](blobHandler)
		mockClient.EXPECT().NewListBlobsFlatPager(gomock.Any(), gomock.Any()).Return(blobPager).Times(2)

		mockClient.EXPECT().DownloadBuffer(gomock.Any(), testString, testString, gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()

		client := storage.NewClient(mockClient)

		var output []byte
		buffer := bytes.NewBuffer(output)
		logger := log.New()
		logger.SetOutput(buffer)

		ctx := context.Background()

		// WHEN
		err := Run(ctx, client, log.NewEntry(logger), time.Now)

		// THEN
		assert.NoError(t, err)
		assert.Contains(t, buffer.String(), fmt.Sprintf("Downloading blob %s", testString))
	})
}
