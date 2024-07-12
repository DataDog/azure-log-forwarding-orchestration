package storage_test

import (
	"context"
	"errors"
	"fmt"
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

func newPagingHandler(items [][]*service.ContainerItem, fetcherError error) runtime.PagingHandler[azblob.ListContainersResponse] {
	counter := 0
	return runtime.PagingHandler[azblob.ListContainersResponse]{
		Fetcher: func(ctx context.Context, response *azblob.ListContainersResponse) (azblob.ListContainersResponse, error) {
			var containersResponse azblob.ListContainersResponse
			if fetcherError != nil {
				return azblob.ListContainersResponse{}, fetcherError
			}
			if len(items) == 0 {
				counter++
				return azblob.ListContainersResponse{}, nil
			}
			containersResponse = azblob.ListContainersResponse{
				ListContainersSegmentResponse: service.ListContainersSegmentResponse{
					ContainerItems: items[counter],
				},
			}
			counter++
			return containersResponse, nil
		},
		More: func(response azblob.ListContainersResponse) bool {
			return counter < len(items)
		},
	}
}

func newContainerItem(name string) *service.ContainerItem {
	return &service.ContainerItem{
		Name: to.StringPtr(name),
	}
}

func getContainersMatchingPrefix(t *testing.T, ctx context.Context, prefix string, responses [][]*service.ContainerItem, fetcherError error) ([]*service.ContainerItem, error) {
	ctrl := gomock.NewController(t)
	handler := newPagingHandler(responses, fetcherError)

	pager := runtime.NewPager[azblob.ListContainersResponse](handler)

	mockClient := mocks.NewMockAzureBlobClient(ctrl)
	mockClient.EXPECT().NewListContainersPager(gomock.Any()).Return(pager)

	client := storage.NewClient(mockClient)

	it := client.GetContainersMatchingPrefix(prefix)

	var results []*service.ContainerItem
	for v, ok, err := it.Next(ctx); ok || err != nil; v, ok, err = it.Next(ctx) {
		if err != nil {
			return nil, fmt.Errorf("error getting next container: %v", err)
		}
		for _, container := range v {
			results = append(results, container)
		}
	}
	return results, nil
}

func TestGetContainersMatchingPrefix(t *testing.T) {
	t.Parallel()

	t.Run("returns names of containers", func(t *testing.T) {
		t.Parallel() // GIVEN
		testString := "test"
		firstPage := []*service.ContainerItem{
			newContainerItem(testString),
			newContainerItem(testString),
		}

		// WHEN
		results, err := getContainersMatchingPrefix(t, context.Background(), storage.LogContainerPrefix, [][]*service.ContainerItem{firstPage}, nil)

		// THEN
		assert.NoError(t, err)
		assert.Len(t, results, 2)
		assert.Equal(t, testString, *results[0].Name)
		assert.Equal(t, testString, *results[1].Name)
	})

	t.Run("returns empty array", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		containers := [][]*service.ContainerItem{}

		// WHEN
		results, err := getContainersMatchingPrefix(t, context.Background(), storage.LogContainerPrefix, containers, nil)

		// THEN
		assert.NoError(t, err)
		assert.Len(t, results, 0)
	})

	t.Run("error response", func(t *testing.T) {
		t.Parallel()
		// GIVEN

		testString := "test"
		fetcherError := errors.New(testString)
		containers := [][]*service.ContainerItem{}

		// WHEN
		_, err := getContainersMatchingPrefix(t, context.Background(), storage.LogContainerPrefix, containers, fetcherError)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, fmt.Sprintf("%v", err), testString)
	})

	t.Run("multiple pages", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testString := "test"
		firstPage := []*service.ContainerItem{
			newContainerItem(testString),
		}
		secondPage := []*service.ContainerItem{
			newContainerItem(testString),
		}
		pages := [][]*service.ContainerItem{firstPage, secondPage}

		// WHEN
		results, err := getContainersMatchingPrefix(t, context.Background(), storage.LogContainerPrefix, pages, nil)

		// THEN
		assert.NoError(t, err)
		assert.Len(t, results, 2)
		assert.Equal(t, testString, *results[0].Name)
		assert.Equal(t, testString, *results[1].Name)
	})
}
