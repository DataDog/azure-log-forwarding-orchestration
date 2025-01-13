package main

import (
	// stdlib
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/go-autorest/autorest/to"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	// datadog
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/collections"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/cursor"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	datadogmocks "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs/mocks"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/metrics"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	storagemocks "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage/mocks"
)

func getLogWithContent(content string) []byte {
	return []byte("{ \"time\": \"2024-08-21T15:12:24Z\", \"resourceId\": \"/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':'','roleInstance':'BD28A314-638598491096328853','message':'" + content + "','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}")
}

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

func getBlobName(name string) string {
	return "resourceId=/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/" + name + "/y=2024/m=10/d=28/h=16/m=00/PT1H.json"
}

func newBlobItem(name string, contentLength int64) *container.BlobItem {
	now := time.Now()
	blobName := getBlobName(name)
	return &container.BlobItem{
		Name: to.StringPtr(blobName),
		Properties: &container.BlobProperties{
			ContentLength: to.Int64Ptr(contentLength),
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

func mockedRun(t *testing.T, containers []*service.ContainerItem, blobs []*container.BlobItem, getDownloadResp func(*azblob.DownloadStreamOptions) azblob.DownloadStreamResponse, cursorResp azblob.DownloadStreamResponse, uploadFunc func(context.Context, string, string, []byte, *azblob.UploadBufferOptions) (azblob.UploadBufferResponse, error)) ([]datadogV2.HTTPLogItem, error) {
	ctrl := gomock.NewController(t)
	mockClient := storagemocks.NewMockAzureBlobClient(ctrl)

	containerHandler := collections.NewPagingHandler[[]*service.ContainerItem, azblob.ListContainersResponse]([][]*service.ContainerItem{containers}, nil, getListContainersResponse)
	containerPager := runtime.NewPager[azblob.ListContainersResponse](containerHandler)
	mockClient.EXPECT().NewListContainersPager(gomock.Any()).Return(containerPager)

	blobHandler := collections.NewPagingHandler[[]*container.BlobItem, azblob.ListBlobsFlatResponse]([][]*container.BlobItem{blobs}, nil, getListBlobsFlatResponse)
	blobPager := runtime.NewPager[azblob.ListBlobsFlatResponse](blobHandler)
	mockClient.EXPECT().NewListBlobsFlatPager(gomock.Any(), gomock.Any()).Return(blobPager).Times(len(containers))

	mockClient.EXPECT().DownloadStream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, containerName string, blobName string, o *azblob.DownloadStreamOptions) (azblob.DownloadStreamResponse, error) {
		if blobName == cursor.BlobName {
			return cursorResp, nil
		}
		if strings.Contains(blobName, "metrics_") {
			resp := azblob.DownloadStreamResponse{}
			resp.Body = io.NopCloser(strings.NewReader(""))
			return resp, nil
		}
		return getDownloadResp(o), nil
	})

	mockClient.EXPECT().UploadBuffer(gomock.Any(), storage.ForwarderContainer, gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(uploadFunc).AnyTimes()

	var resp azblob.CreateContainerResponse
	mockClient.EXPECT().CreateContainer(gomock.Any(), storage.ForwarderContainer, gomock.Any()).Return(resp, nil).Times(2)

	client := storage.NewClient(mockClient)

	var submittedLogs []datadogV2.HTTPLogItem
	mockDDClient := datadogmocks.NewMockDatadogLogsSubmitter(ctrl)
	mockDDClient.EXPECT().SubmitLog(gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(2).DoAndReturn(func(ctx context.Context, body []datadogV2.HTTPLogItem, o ...datadogV2.SubmitLogOptionalParameters) (interface{}, *http.Response, error) {
		submittedLogs = append(submittedLogs, body...)
		return nil, nil, nil
	})

	logClient := logs.NewClient(mockDDClient)

	var output []byte
	buffer := bytes.NewBuffer(output)
	logger := log.New()
	logger.SetOutput(buffer)

	ctx := context.Background()

	err := run(ctx, client, []*logs.Client{logClient}, log.NewEntry(logger), time.Now)
	return submittedLogs, err
}

func TestRun(t *testing.T) {
	t.Parallel()

	t.Run("execute the basic functionality", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testString := "test"
		validLog := getLogWithContent(testString)
		expectedBytesForLog := len(validLog) + 1 // +1 for newline

		containerPage := []*service.ContainerItem{
			newContainerItem("insights-logs-functionapplogs"),
		}
		blobPage := []*container.BlobItem{
			newBlobItem("testA", int64(expectedBytesForLog)),
			newBlobItem("testB", int64(expectedBytesForLog)),
		}

		getDownloadResp := func(o *azblob.DownloadStreamOptions) azblob.DownloadStreamResponse {
			resp := azblob.DownloadStreamResponse{}
			resp.Body = io.NopCloser(strings.NewReader(string(validLog)))
			return resp
		}

		cursorResp := azblob.DownloadStreamResponse{}
		cursorResp.Body = io.NopCloser(strings.NewReader(""))

		var uploadedMetrics []byte
		uploadFunc := func(ctx context.Context, containerName string, blobName string, content []byte, o *azblob.UploadBufferOptions) (azblob.UploadBufferResponse, error) {
			if strings.Contains(blobName, "metrics_") {
				uploadedMetrics = append(uploadedMetrics, content...)
			}
			return azblob.UploadBufferResponse{}, nil
		}

		// WHEN
		submittedLogs, err := mockedRun(t, containerPage, blobPage, getDownloadResp, cursorResp, uploadFunc)

		// THEN
		assert.NoError(t, err)

		finalMetrics, err := metrics.FromBytes(uploadedMetrics)
		assert.NoError(t, err)
		totalLoad := 0
		totalBytes := 0
		for _, metric := range finalMetrics {
			for _, value := range metric.ResourceLogVolumes {
				totalLoad += int(value)
			}
			for _, value := range metric.ResourceLogBytes {
				totalBytes += int(value)
			}
		}
		assert.Equal(t, len(blobPage), totalLoad)
		assert.Equal(t, len(blobPage)*(expectedBytesForLog), totalBytes)
		assert.Len(t, submittedLogs, len(blobPage))

		for _, logItem := range submittedLogs {
			assert.Equal(t, "azure", *logItem.Ddsource)
			assert.Contains(t, *logItem.Ddtags, "forwarder:lfo")
		}
	})

	t.Run("continues processing on errors", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testString := "test"
		validLog := getLogWithContent(testString)
		expectedBytesForLog := len(validLog) + 1 // +1 for newline

		containerPage := []*service.ContainerItem{
			newContainerItem("insights-logs-functionapplogs"),
		}
		blobPage := []*container.BlobItem{
			newBlobItem("testA", int64(expectedBytesForLog)),
			newBlobItem("testB", int64(expectedBytesForLog)),
		}

		firstBlob := true

		getDownloadResp := func(o *azblob.DownloadStreamOptions) azblob.DownloadStreamResponse {
			resp := azblob.DownloadStreamResponse{}
			if firstBlob {
				firstBlob = false
				resp.Body = io.NopCloser(strings.NewReader("invalid"))
			} else {
				resp.Body = io.NopCloser(strings.NewReader(string(validLog)))
			}
			return resp
		}

		cursorResp := azblob.DownloadStreamResponse{}
		cursorResp.Body = io.NopCloser(strings.NewReader(""))

		var uploadedMetrics []byte
		uploadFunc := func(ctx context.Context, containerName string, blobName string, content []byte, o *azblob.UploadBufferOptions) (azblob.UploadBufferResponse, error) {
			if strings.Contains(blobName, "metrics_") {
				uploadedMetrics = append(uploadedMetrics, content...)
			}
			return azblob.UploadBufferResponse{}, nil
		}

		// WHEN
		submittedLogs, err := mockedRun(t, containerPage, blobPage, getDownloadResp, cursorResp, uploadFunc)

		// THEN
		assert.ErrorIs(t, err, logs.ErrInvalidJavaScript)

		finalMetrics, err := metrics.FromBytes(uploadedMetrics)
		assert.NoError(t, err)
		totalLoad := 0
		totalBytes := 0
		for _, metric := range finalMetrics {
			for _, value := range metric.ResourceLogVolumes {
				totalLoad += int(value)
			}
			for _, value := range metric.ResourceLogBytes {
				totalBytes += int(value)
			}
		}
		assert.Equal(t, len(blobPage)-1, totalLoad)
		assert.Equal(t, (len(blobPage)-1)*(expectedBytesForLog), totalBytes)
		assert.Len(t, submittedLogs, len(blobPage)-1)

		for _, logItem := range submittedLogs {
			assert.Equal(t, "azure", *logItem.Ddsource)
			assert.Contains(t, *logItem.Ddtags, "forwarder:lfo")
		}
	})
}

func TestProcessLogs(t *testing.T) {
	t.Parallel()

	t.Run("submits logs to dd", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		validLog := "{ \"time\": \"2024-08-21T15:12:24Z\", \"resourceId\": \"/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':'','roleInstance':'BD28A314-638598491096328853','message':'LoggerFilterOptions\\n{\\n  \\'MinLevel\\': \\'None\\',\\n  \\'Rules\\': [\\n    {\\n      \\'ProviderName\\': null,\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'None\\',\\n      \\'Filter\\': null\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Logging.ApplicationInsights.ApplicationInsightsLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'Trace\\',\\n      \\'Filter\\': null\\n    }\\n  ]\\n}','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}\n"
		var content string
		for range 3 {
			content += validLog
		}
		reader := io.NopCloser(strings.NewReader(content))

		ctrl := gomock.NewController(t)
		var submittedLogs []datadogV2.HTTPLogItem
		mockDDClient := datadogmocks.NewMockDatadogLogsSubmitter(ctrl)
		mockDDClient.EXPECT().SubmitLog(gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(2).DoAndReturn(func(ctx context.Context, body []datadogV2.HTTPLogItem, o ...datadogV2.SubmitLogOptionalParameters) (interface{}, *http.Response, error) {
			submittedLogs = append(submittedLogs, body...)
			return nil, nil, nil
		})

		datadogClient := logs.NewClient(mockDDClient)
		defer datadogClient.Flush(context.Background())

		eg, egCtx := errgroup.WithContext(context.Background())

		logsCh := make(chan *logs.Log, 100)
		volumeCh := make(chan string, 100)
		bytesCh := make(chan resourceBytes, 100)

		var output []byte
		buffer := bytes.NewBuffer(output)
		logger := log.New()
		logger.SetOutput(buffer)

		// WHEN
		eg.Go(func() error {
			defer close(volumeCh)
			defer close(bytesCh)
			return processLogs(egCtx, datadogClient, log.NewEntry(logger), logsCh, volumeCh, bytesCh)
		})
		eg.Go(func() error {
			defer close(logsCh)
			_, _, err := parseLogs(reader, "insights-logs-functionapplogs", logsCh)
			return err
		})
		err := eg.Wait()

		// THEN
		assert.NoError(t, err)
		assert.Len(t, submittedLogs, 3)
		for _, logItem := range submittedLogs {
			assert.Equal(t, "azure", *logItem.Ddsource)
			assert.Contains(t, *logItem.Ddtags, "forwarder:lfo")
		}
	})

	t.Run("logs when dropping a too large log", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		oneHundredAs := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		var content string
		for range logs.MaxPayloadSize / 100 {
			content += oneHundredAs
		}
		invalidLog := getLogWithContent(content)
		reader := io.NopCloser(strings.NewReader(string(invalidLog)))

		ctrl := gomock.NewController(t)
		mockDDClient := datadogmocks.NewMockDatadogLogsSubmitter(ctrl)

		datadogClient := logs.NewClient(mockDDClient)
		defer datadogClient.Flush(context.Background())

		eg, egCtx := errgroup.WithContext(context.Background())

		logsCh := make(chan *logs.Log, 100)
		volumeCh := make(chan string, 100)
		bytesCh := make(chan resourceBytes, 100)

		var output []byte
		buffer := bytes.NewBuffer(output)
		logger := log.New()
		logger.SetOutput(buffer)

		containerName := "insights-logs-functionapplogs"

		var invalidLogError logs.TooLargeError
		parsedLog, err := logs.NewLog(invalidLog, containerName)
		require.NoError(t, err)

		// WHEN
		eg.Go(func() error {
			defer close(volumeCh)
			defer close(bytesCh)
			return processLogs(egCtx, datadogClient, log.NewEntry(logger), logsCh, volumeCh, bytesCh)
		})
		eg.Go(func() error {
			defer close(logsCh)
			_, _, err := parseLogs(reader, containerName, logsCh)
			return err
		})

		err = eg.Wait()

		// THEN
		assert.False(t, parsedLog.IsValid())
		assert.ErrorAs(t, err, &invalidLogError)
		assert.Contains(t, string(buffer.Bytes()), "large log from")
	})
}

func TestParseLogs(t *testing.T) {
	t.Parallel()

	t.Run("creates a Log from raw log", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		validLog := getLogWithContent("test")
		var content string
		for range 3 {
			content += string(validLog) + "\n"
		}
		reader := io.NopCloser(strings.NewReader(content))

		eg, _ := errgroup.WithContext(context.Background())
		var got []*logs.Log

		logsChannel := make(chan *logs.Log, 100)

		// WHEN
		eg.Go(func() error {
			for log := range logsChannel {
				got = append(got, log)
			}
			return nil
		})
		eg.Go(func() error {
			defer close(logsChannel)
			_, _, err := parseLogs(reader, "insights-logs-functionapplogs", logsChannel)
			return err
		})
		err := eg.Wait()

		// THEN
		assert.NoError(t, err)
		assert.Len(t, got, 3)
	})
}

func TestCursors(t *testing.T) {
	t.Parallel()

	t.Run("works with aks logs", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		workingDir, err := os.Getwd()
		require.NoError(t, err)

		originalLogData, err := os.ReadFile(fmt.Sprintf("%s/fixtures/aks_logs.json", workingDir))
		require.NoError(t, err)

		containerName := "insights-logs-kube-audit"
		blobName := "aks_logs.json"

		containerPage := []*service.ContainerItem{
			newContainerItem(containerName),
		}

		n := 5 // Number of times to execute

		var currentLogData []byte
		now := time.Now()

		lastCursor := cursor.NewCursors(nil)

		for i := 0; i < n; i++ {
			// REPEATED GIVEN
			currentLogData = append(currentLogData, originalLogData...)

			blobItem := &container.BlobItem{
				Name: to.StringPtr(blobName),
				Properties: &container.BlobProperties{
					ContentLength: to.Int64Ptr(int64(len(currentLogData))),
					CreationTime:  &now,
				},
			}

			cursorResp := azblob.DownloadStreamResponse{}
			cursorResp.Body = io.NopCloser(strings.NewReader(""))

			var output []byte
			buffer := bytes.NewBuffer(output)
			logger := log.New()
			logger.SetOutput(buffer)

			uploadFunc := func(ctx context.Context, containerName string, blobName string, content []byte, o *azblob.UploadBufferOptions) (azblob.UploadBufferResponse, error) {
				if blobName == cursor.BlobName {
					lastCursor = cursor.FromBytes(content, log.NewEntry(logger))
					require.NoError(t, err)
				}
				return azblob.UploadBufferResponse{}, nil
			}

			getDownloadResp := func(o *azblob.DownloadStreamOptions) azblob.DownloadStreamResponse {
				resp := azblob.DownloadStreamResponse{}
				resp.Body = io.NopCloser(strings.NewReader(string(currentLogData[o.Range.Offset:])))
				return resp
			}

			// WHEN
			submittedLogs, err := mockedRun(t, containerPage, []*container.BlobItem{blobItem}, getDownloadResp, cursorResp, uploadFunc)

			// THEN
			assert.NoError(t, err)

			assert.Equal(t, int64(len(currentLogData)), lastCursor.GetCursor(containerName, blobName))

			for _, logItem := range submittedLogs {
				assert.Equal(t, "azure", *logItem.Ddsource)
				assert.Contains(t, *logItem.Ddtags, "forwarder:lfo")
			}
		}
	})

	t.Run("works with function app logs", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		workingDir, err := os.Getwd()
		require.NoError(t, err)

		originalLogData, err := os.ReadFile(fmt.Sprintf("%s/fixtures/function_app_logs.json", workingDir))
		require.NoError(t, err)

		containerName := "insights-logs-functionapplogs"
		blobName := "function_app_logs.json"

		containerPage := []*service.ContainerItem{
			newContainerItem(containerName),
		}

		n := 5 // Number of times to execute

		var currentLogData []byte
		now := time.Now()

		lastCursor := cursor.NewCursors(nil)

		for i := 0; i < n; i++ {
			// REPEATED GIVEN
			currentLogData = append(currentLogData, originalLogData...)

			blobItem := &container.BlobItem{
				Name: to.StringPtr(blobName),
				Properties: &container.BlobProperties{
					ContentLength: to.Int64Ptr(int64(len(currentLogData))),
					CreationTime:  &now,
				},
			}

			cursorResp := azblob.DownloadStreamResponse{}
			cursorResp.Body = io.NopCloser(strings.NewReader(""))

			var output []byte
			buffer := bytes.NewBuffer(output)
			logger := log.New()
			logger.SetOutput(buffer)

			uploadFunc := func(ctx context.Context, containerName string, blobName string, content []byte, o *azblob.UploadBufferOptions) (azblob.UploadBufferResponse, error) {
				if blobName == cursor.BlobName {
					lastCursor = cursor.FromBytes(content, log.NewEntry(logger))
					require.NoError(t, err)
				}
				return azblob.UploadBufferResponse{}, nil
			}

			getDownloadResp := func(o *azblob.DownloadStreamOptions) azblob.DownloadStreamResponse {
				resp := azblob.DownloadStreamResponse{}
				resp.Body = io.NopCloser(strings.NewReader(string(currentLogData[o.Range.Offset:])))
				return resp
			}

			// WHEN
			submittedLogs, err := mockedRun(t, containerPage, []*container.BlobItem{blobItem}, getDownloadResp, cursorResp, uploadFunc)

			// THEN
			assert.NoError(t, err)

			assert.Equal(t, int64(len(currentLogData)), lastCursor.GetCursor(containerName, blobName))

			for _, logItem := range submittedLogs {
				assert.Equal(t, "azure", *logItem.Ddsource)
				assert.Contains(t, *logItem.Ddtags, "forwarder:lfo")
			}
		}
	})
}

// TestRunMain exists for performance testing purposes.
func TestRunMain(t *testing.T) {
	t.Parallel()

	t.Run("run main", func(t *testing.T) {
		t.Parallel()
		if os.Getenv("CI") != "" {
			t.Skip("Skipping testing in CI environment")
		}
		main()
	})
}
