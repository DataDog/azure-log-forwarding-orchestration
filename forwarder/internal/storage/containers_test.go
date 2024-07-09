package storage_test

import (
	"context"
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
	// Given
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

	client := storage.NewClientWithClient(mockClient)

	// When
	containers, err := client.GetContainersMatchingPrefix(context.Background(), storage.LogContainerPrefix)

	// Then
	assert.NoError(t, err)
	assert.NotNil(t, containers)
	assert.Len(t, containers, 2)
	assert.Equal(t, testString, *containers[0])
	assert.Equal(t, testString, *containers[1])
}
