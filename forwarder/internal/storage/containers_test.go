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
	"golang.org/x/sync/errgroup"
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
	eg, egCtx := errgroup.WithContext(context.Background())
	resultChan := make(chan []*string, 1)

	// WHEN
	err := client.GetContainersMatchingPrefix(egCtx, eg, storage.LogContainerPrefix, resultChan)

	// THEN
	assert.NoError(t, err)
	err = eg.Wait()
	assert.NoError(t, err)

	results := []*string{}
	select {
	case result := <-resultChan:
		results = append(results, result...)
	}
	close(resultChan)
	assert.NoError(t, err)
	assert.NotNil(t, results)
	assert.Len(t, results, 2)
	assert.Equal(t, testString, *results[0])
	assert.Equal(t, testString, *results[1])
}

func TestGetContainersMatchingPrefix_ReturnsEmptyArray(t *testing.T) {
	// GIVEN
	ctrl := gomock.NewController(t)

	handler := runtime.PagingHandler[azblob.ListContainersResponse]{
		Fetcher: func(ctx context.Context, response *azblob.ListContainersResponse) (azblob.ListContainersResponse, error) {
			return azblob.ListContainersResponse{}, nil
		},
		More: func(response azblob.ListContainersResponse) bool {
			return false
		},
	}

	pager := runtime.NewPager[azblob.ListContainersResponse](handler)

	mockClient := mocks.NewMockAzureBlobClient(ctrl)
	mockClient.EXPECT().NewListContainersPager(gomock.Any()).Return(pager)

	client := storage.NewClientWithAzBlobClient(mockClient)
	eg, egCtx := errgroup.WithContext(context.Background())
	resultChan := make(chan []*string, 1)

	// WHEN
	err := client.GetContainersMatchingPrefix(egCtx, eg, storage.LogContainerPrefix, resultChan)

	// THEN
	assert.NoError(t, err)
	err = eg.Wait()
	assert.NoError(t, err)

	results := []*string{}
	select {
	case result := <-resultChan:
		results = append(results, result...)
	}
	close(resultChan)
	assert.NoError(t, err)
	assert.Len(t, results, 0)
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
	eg, egCtx := errgroup.WithContext(context.Background())
	resultChan := make(chan []*string, 1)

	// WHEN
	err := client.GetContainersMatchingPrefix(egCtx, eg, storage.LogContainerPrefix, resultChan)

	// THEN

	close(resultChan)
	assert.Error(t, err)
	assert.EqualError(t, err, testString)
}
