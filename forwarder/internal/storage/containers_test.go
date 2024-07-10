package storage_test

import (
	"context"
	"errors"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/go-autorest/autorest/to"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"testing"
)

func TestGetContainersMatchingPrefix_ReturnsNamesOfContainers(t *testing.T) {
	// GIVEN
	ctrl := gomock.NewController(t)

	testString := "test"

	handler := runtime.PagingHandler[azblob.ListContainersResponse]{
		Fetcher: func(ctx context.Context, response *azblob.ListContainersResponse) (azblob.ListContainersResponse, error) {
			return azblob.ListContainersResponse{
				ListContainersSegmentResponse: service.ListContainersSegmentResponse{
					ContainerItems: []*service.ContainerItem{
						{Name: to.StringPtr(testString)},
						{Name: to.StringPtr(testString)},
					},
				},
			}, nil
		},
		More: func(response azblob.ListContainersResponse) bool {
			return false
		},
	}

	pager := runtime.NewPager[azblob.ListContainersResponse](handler)

	mockClient := mocks.NewMockAzureBlobClient(ctrl)
	mockClient.EXPECT().NewListContainersPager(gomock.Any()).Return(pager)

	client := storage.NewClientWithAzBlobClient(mockClient)

	// WHEN
	containers, err := client.GetContainersMatchingPrefix(context.Background(), storage.LogContainerPrefix)

	// THEN
	assert.NoError(t, err)
	assert.NotNil(t, containers)
	assert.Len(t, containers, 2)
	assert.Equal(t, testString, *containers[0])
	assert.Equal(t, testString, *containers[1])
}

func TestGetContainersMatchingPrefix_ReturnsEmptyArray(t *testing.T) {
	// GIVEN
	ctrl := gomock.NewController(t)

	handler := runtime.PagingHandler[azblob.ListContainersResponse]{
		Fetcher: func(ctx context.Context, response *azblob.ListContainersResponse) (azblob.ListContainersResponse, error) {
			return azblob.ListContainersResponse{
				ListContainersSegmentResponse: service.ListContainersSegmentResponse{
					ContainerItems: []*service.ContainerItem{},
				},
			}, nil
		},
		More: func(response azblob.ListContainersResponse) bool {
			return false
		},
	}

	pager := runtime.NewPager[azblob.ListContainersResponse](handler)

	mockClient := mocks.NewMockAzureBlobClient(ctrl)
	mockClient.EXPECT().NewListContainersPager(gomock.Any()).Return(pager)

	client := storage.NewClientWithAzBlobClient(mockClient)

	// WHEN
	containers, err := client.GetContainersMatchingPrefix(context.Background(), storage.LogContainerPrefix)

	// THEN
	assert.NoError(t, err)
	assert.NotNil(t, containers)
	assert.Len(t, containers, 0)
}

func TestGetContainersMatchingPrefix_ErrorResponse(t *testing.T) {
	// GIVEN
	ctrl := gomock.NewController(t)

	testString := "test"

	handler := runtime.PagingHandler[azblob.ListContainersResponse]{
		Fetcher: func(ctx context.Context, response *azblob.ListContainersResponse) (azblob.ListContainersResponse, error) {
			return azblob.ListContainersResponse{}, errors.New(testString)
		},
		More: func(response azblob.ListContainersResponse) bool {
			return false
		},
	}

	pager := runtime.NewPager[azblob.ListContainersResponse](handler)

	mockClient := mocks.NewMockAzureBlobClient(ctrl)
	mockClient.EXPECT().NewListContainersPager(gomock.Any()).Return(pager)

	client := storage.NewClientWithAzBlobClient(mockClient)

	// WHEN
	_, err := client.GetContainersMatchingPrefix(context.Background(), storage.LogContainerPrefix)

	// THEN
	assert.Error(t, err)
	assert.EqualError(t, err, testString)
}
