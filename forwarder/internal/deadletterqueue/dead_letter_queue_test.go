package deadletterqueue_test

import (
	// stdlib
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	// datadog
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/deadletterqueue"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	logmocks "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs/mocks"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage/mocks"
)

func MockLogger() (*log.Entry, *bytes.Buffer) {
	var output []byte
	buffer := bytes.NewBuffer(output)
	logger := log.New()
	logger.SetOutput(buffer)
	return log.NewEntry(logger), buffer
}

func TestLoadDLQ(t *testing.T) {
	t.Parallel()

	t.Run("reads dead letter queue from azure", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testMessage := "test"
		dlq, err := deadletterqueue.FromBytes(nil, []byte("[]"))
		queue := make([]datadogV2.HTTPLogItem, 0)
		queue = append(queue, datadogV2.HTTPLogItem{
			Message: testMessage,
		})
		dlq.Add(queue)
		data, err := dlq.Bytes()
		reader := io.NopCloser(bytes.NewReader(data))
		response := azblob.DownloadStreamResponse{
			DownloadResponse: blob.DownloadResponse{
				Body: reader,
			},
		}

		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockAzureBlobClient(ctrl)
		mockClient.EXPECT().DownloadStream(gomock.Any(), storage.ForwarderContainer, deadletterqueue.BlobName, nil).Return(response, nil)

		storageClient := storage.NewClient(mockClient)

		datadogClient := logmocks.NewMockDatadogLogsSubmitter(ctrl)
		logsClient := logs.NewClient(datadogClient)

		// WHEN
		got, err := deadletterqueue.Load(context.Background(), storageClient, logsClient)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, got)
		gotLogs := dlq.GetQueue()
		assert.Len(t, gotLogs, 1)
		assert.Equal(t, testMessage, gotLogs[0].Message)
	})

	t.Run("gives new dead letter queue when file cannot be found", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockAzureBlobClient(ctrl)
		resp := http.Response{
			Body:       io.NopCloser(bytes.NewBufferString("test")),
			StatusCode: 404,
		}
		respErr := runtime.NewResponseErrorWithErrorCode(&resp, "BlobNotFound")
		mockClient.EXPECT().DownloadStream(gomock.Any(), storage.ForwarderContainer, deadletterqueue.BlobName, nil).Return(azblob.DownloadStreamResponse{}, respErr)

		storageClient := storage.NewClient(mockClient)

		var output []byte
		buffer := bytes.NewBuffer(output)
		logger := log.New()
		logger.SetOutput(buffer)

		// WHEN
		got, err := deadletterqueue.Load(context.Background(), storageClient, nil)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, got)
		assert.Len(t, got.GetQueue(), 0)
	})
}

func TestSaveDLQ(t *testing.T) {
	t.Parallel()

	t.Run("saves dead letter queue to azure", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		response := azblob.UploadBufferResponse{}
		createContainerResponse := azblob.CreateContainerResponse{}

		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockAzureBlobClient(ctrl)
		mockClient.EXPECT().CreateContainer(gomock.Any(), storage.ForwarderContainer, nil).Return(createContainerResponse, nil)
		mockClient.EXPECT().UploadBuffer(gomock.Any(), storage.ForwarderContainer, deadletterqueue.BlobName, gomock.Any(), gomock.Any()).Return(response, nil)

		client := storage.NewClient(mockClient)

		datadogClient := logmocks.NewMockDatadogLogsSubmitter(ctrl)
		logsClient := logs.NewClient(datadogClient)

		dlq, err := deadletterqueue.FromBytes(logsClient, []byte("[]"))
		require.NoError(t, err)

		// WHEN
		err = dlq.Save(context.Background(), client)

		// THEN
		assert.NoError(t, err)
	})
}
