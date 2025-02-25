package storage_test

import (
	// stdlib
	"bytes"
	"context"
	"errors"
	"testing"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/go-autorest/autorest/to"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/collections"
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

func getLogContainers(t *testing.T, ctx context.Context, responses [][]*service.ContainerItem, fetcherError error) []storage.Container {
	ctrl := gomock.NewController(t)
	handler := collections.NewPagingHandler(responses, fetcherError, getListContainersResponse)

	pager := runtime.NewPager(handler)

	mockClient := mocks.NewMockAzureBlobClient(ctrl)
	mockClient.EXPECT().NewListContainersPager(gomock.Any()).Return(pager)

	client := storage.NewClient(mockClient)

	var output []byte
	buffer := bytes.NewBuffer(output)
	logger := log.New()
	logger.SetOutput(buffer)

	var containers []storage.Container
	seq := client.GetLogContainers(ctx, log.NewEntry(logger))
	for item := range seq {
		containers = append(containers, item)
	}
	return containers
}

func TestGetLogContainers(t *testing.T) {
	t.Parallel()

	t.Run("returns names of containers", func(t *testing.T) {
		t.Parallel() // GIVEN
		testString := "test"
		firstPage := []*service.ContainerItem{
			newContainerItem(testString),
			newContainerItem(testString),
		}

		// WHEN
		results := getLogContainers(t, context.Background(), [][]*service.ContainerItem{firstPage}, nil)

		// THEN
		assert.Len(t, results, 2)
		assert.Equal(t, testString, results[0].Name)
		assert.Equal(t, testString, results[1].Name)
	})

	t.Run("returns empty array on empty array", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		containers := [][]*service.ContainerItem{}

		// WHEN
		results := getLogContainers(t, context.Background(), containers, nil)

		// THEN
		assert.Len(t, results, 0)
	})

	t.Run("error response", func(t *testing.T) {
		t.Parallel()
		// GIVEN

		testString := "test"
		fetcherError := errors.New(testString)
		containers := [][]*service.ContainerItem{}

		// WHEN
		results := getLogContainers(t, context.Background(), containers, fetcherError)

		// THEN
		assert.Len(t, results, 0)
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
		results := getLogContainers(t, context.Background(), pages, nil)

		// THEN
		assert.Len(t, results, 2)
		assert.Equal(t, testString, results[0].Name)
		assert.Equal(t, testString, results[1].Name)
	})

	t.Run("filtered out excluded containers", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		pages := [][]*service.ContainerItem{
			{newContainerItem("test")},
			{newContainerItem(storage.IgnoredContainers[0])},
			{newContainerItem("test2"), newContainerItem(storage.IgnoredContainers[1])},
		}

		// WHEN
		results := getLogContainers(t, context.Background(), pages, nil)

		// THEN
		assert.Len(t, results, 2)
		assert.Equal(t, "test", results[0].Name)
		assert.Equal(t, "test2", results[1].Name)
	})
}

func TestCategory(t *testing.T) {
	t.Parallel()

	t.Run("parses category from container name", func(t *testing.T) {
		t.Parallel() // GIVEN
		c := storage.Container{Name: "insights-logs-functionapplogs"}
		want := "functionapplogs"

		// WHEN
		got := c.Category()
		// THEN
		assert.Equal(t, want, got)
	})
}
