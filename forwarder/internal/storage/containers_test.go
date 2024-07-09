package storage_test

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"gopkg.in/dnaeon/go-vcr.v3/recorder"
	"path"
	"testing"
)

func TestGetContainersMatchingPrefix_ReturnsArrayOfContainerNames(t *testing.T) {
	// GIVEN
	rec, err := recorder.New(path.Join("fixtures", "containers_list_with_prefix"))
	assert.NoErrorf(t, err, "failed creating recorder ")
	defer rec.Stop()

	options := &azblob.ClientOptions{}
	options.Transport = rec.GetDefaultClient()

	client, err := azblob.NewClientWithNoCredential("https://mattlogger.blob.core.windows.net/", options)
	assert.NoError(t, err)

	// WHEN
	containers, err := storage.GetContainersMatchingPrefix(context.Background(), client, storage.LogPrefix)

	// THEN
	assert.NoError(t, err)
	assert.NotNil(t, containers)
	assert.Greater(t, len(containers), 0)
	assert.Equal(t, "insights-logs-functionapplogs", *containers[0])
}

func TestGetContainersMatchingPrefix_ReturnsEmptyWhenNoMatchesFound(t *testing.T) {
	// GIVEN
	rec, err := recorder.New(path.Join("fixtures", "containers_list_with_invalid_prefix"))
	assert.NoErrorf(t, err, "failed creating recorder ")
	defer rec.Stop()

	options := &azblob.ClientOptions{}
	options.Transport = rec.GetDefaultClient()

	client, err := azblob.NewClientWithNoCredential("https://mattlogger.blob.core.windows.net/", options)
	assert.NoError(t, err)

	// WHEN
	containers, err := storage.GetContainersMatchingPrefix(context.Background(), client, "nonexistentprefix")

	// THEN
	assert.NoError(t, err)
	assert.NotNil(t, containers)
	assert.Equal(t, len(containers), 0)
}

func TestMocking(t *testing.T) {
	// GIVEN
	ctrl := gomock.NewController(t)

	mockPager := NewMockAzurePager[azblob.ListContainersResponse](ctrl)
	mockPager.EXPECT().More().Return(false)

	mockClient := NewMockAzureBlobClient(ctrl)
	mockClient.EXPECT().NewListContainersPager(gomock.Any()).Return(mockPager)

}
