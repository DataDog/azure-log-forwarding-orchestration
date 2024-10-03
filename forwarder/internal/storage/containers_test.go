package storage_test

import (
	// stdlib
	"context"
	"errors"
	"fmt"
	"testing"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/go-autorest/autorest/to"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	// datadog
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage/mocks"
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

func getContainersMatchingPrefix(t *testing.T, ctx context.Context, prefix string, responses [][]*service.ContainerItem, fetcherError error) ([]*service.ContainerItem, error) {
	ctrl := gomock.NewController(t)
	handler := storage.NewPagingHandler[[]*service.ContainerItem, azblob.ListContainersResponse](responses, fetcherError, getListContainersResponse)

	pager := runtime.NewPager[azblob.ListContainersResponse](handler)

	mockClient := mocks.NewMockAzureBlobClient(ctrl)
	mockClient.EXPECT().NewListContainersPager(gomock.Any()).Return(pager)

	client := storage.NewClient(mockClient)

	span, ctx := tracer.StartSpanFromContext(context.Background(), "containers.test")
	defer span.Finish()

	it := client.GetContainersMatchingPrefix(ctx, prefix)

	var results []*service.ContainerItem
	for {
		item, err := it.Next(ctx)

		if errors.Is(err, storage.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error getting next container: %w", err)
		}
		results = append(results, item)
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
