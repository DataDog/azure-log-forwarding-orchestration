package main

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/metrics"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"

	"go.uber.org/mock/gomock"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/go-autorest/autorest/to"
	datadogmocks "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs/mocks"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	storagemocks "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage/mocks"
	log "github.com/sirupsen/logrus"

	"github.com/stretchr/testify/assert"
)

var validLog = []byte("{ \"time\": \"2024-08-21T15:12:24Z\", \"resourceId\": \"/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':'','roleInstance':'BD28A314-638598491096328853','message':'LoggerFilterOptions\\n{\\n  \\'MinLevel\\': \\'None\\',\\n  \\'Rules\\': [\\n    {\\n      \\'ProviderName\\': null,\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'None\\',\\n      \\'Filter\\': null\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Logging.ApplicationInsights.ApplicationInsightsLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'Trace\\',\\n      \\'Filter\\': null\\n    }\\n  ]\\n}','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}\n")

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
			ContentLength: to.Int64Ptr(int64(len(validLog))),
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
		mockClient := storagemocks.NewMockAzureBlobClient(ctrl)

		containerHandler := storage.NewPagingHandler[[]*service.ContainerItem, azblob.ListContainersResponse]([][]*service.ContainerItem{containerPage}, nil, getListContainersResponse)
		containerPager := runtime.NewPager[azblob.ListContainersResponse](containerHandler)
		mockClient.EXPECT().NewListContainersPager(gomock.Any()).Return(containerPager)

		blobHandler := storage.NewPagingHandler[[]*container.BlobItem, azblob.ListBlobsFlatResponse]([][]*container.BlobItem{blobPage}, nil, getListBlobsFlatResponse)
		blobPager := runtime.NewPager[azblob.ListBlobsFlatResponse](blobHandler)
		mockClient.EXPECT().NewListBlobsFlatPager(gomock.Any(), gomock.Any()).Return(blobPager).Times(2)

		mockClient.EXPECT().DownloadBuffer(gomock.Any(), testString, testString, gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, containerName string, blobName string, buffer []byte, o *azblob.DownloadBufferOptions) (int64, error) {
			copy(buffer, validLog)
			return int64(len(validLog)), nil
		})

		var uploadedMetrics []byte
		mockClient.EXPECT().UploadBuffer(gomock.Any(), metrics.MetricsContainer, gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, containerName string, blobName string, content []byte, o *azblob.UploadBufferOptions) (azblob.UploadBufferResponse, error) {
				uploadedMetrics = append(uploadedMetrics, content...)
				return azblob.UploadBufferResponse{}, nil
			})

		data := "\n"
		stringReader := strings.NewReader(data)
		reader := ioutil.NopCloser(stringReader)

		var downloadResp azblob.DownloadStreamResponse
		downloadResp.Body = reader
		mockClient.EXPECT().DownloadStream(gomock.Any(), metrics.MetricsContainer, gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, containerName string, blobName string, o *azblob.DownloadStreamOptions) (azblob.DownloadStreamResponse, error) {
			return downloadResp, nil
		})

		var resp azblob.CreateContainerResponse
		mockClient.EXPECT().CreateContainer(gomock.Any(), metrics.MetricsContainer, gomock.Any()).Return(resp, nil)

		client := storage.NewClient(mockClient)

		var submittedLogs []datadogV2.HTTPLogItem
		mockDDClient := datadogmocks.NewMockLogsApiInterface(ctrl)
		mockDDClient.EXPECT().SubmitLog(gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(2).DoAndReturn(func(ctx context.Context, body []datadogV2.HTTPLogItem, o ...datadogV2.SubmitLogOptionalParameters) (interface{}, *http.Response, error) {
			submittedLogs = append(submittedLogs, body...)
			return nil, nil, nil
		})

		datadogClient := logs.NewClient(mockDDClient)

		var output []byte
		buffer := bytes.NewBuffer(output)
		logger := log.New()
		logger.SetOutput(buffer)

		ctx := context.Background()

		// WHEN
		err := Run(ctx, client, datadogClient, log.NewEntry(logger), time.Now)

		// THEN
		assert.NoError(t, err)

		finalMetrics, err := metrics.FromBytes(uploadedMetrics)
		assert.NoError(t, err)
		totalLoad := 0
		for _, metric := range finalMetrics {
			for _, value := range metric.ResourceLogVolumes {
				totalLoad += int(value)
			}
		}
		assert.Equal(t, 2, totalLoad)
		assert.Len(t, submittedLogs, 2)

		for _, logItem := range submittedLogs {
			assert.Equal(t, "azure", *logItem.Ddsource)
			assert.Contains(t, *logItem.Ddtags, "forwarder:lfo")
		}
	})
}
