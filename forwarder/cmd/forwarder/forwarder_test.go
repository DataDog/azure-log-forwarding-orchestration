// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package main

import (
	// stdlib
	"bytes"
	"compress/gzip"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
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
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/docker/go-connections/nat"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	// datadog
	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/collections"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/cursor"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/deadletterqueue"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	logmocks "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs/mocks"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/metrics"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	storagemocks "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage/mocks"
	customtime "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/time"
)

const (
	resourceId   string = "/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING"
	versionTag   string = "test-version"
	azureService string = "azure"
)

func azureTimestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

func getLogWithContent(content string, delay time.Duration) []byte {
	timestamp := time.Now().Add(-delay)
	return []byte("{ \"time\": \"" + azureTimestamp(timestamp) + "\", \"resourceId\": \"/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':'','roleInstance':'BD28A314-638598491096328853','message':'" + content + "','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}")
}

func nullLogger() *log.Logger {
	l := log.New()
	l.SetOutput(io.Discard)
	return l
}

// CustomRoundTripper implements the http.RoundTripper interface
type CustomRoundTripper struct {
	transport   http.RoundTripper
	getResponse func(req *http.Request) (*http.Response, error)
}

// RoundTrip implements the RoundTrip method required by the http.RoundTripper interface
func (c *CustomRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return c.getResponse(req)
}

func newMockPiiScrubber(ctrl *gomock.Controller) *logmocks.MockScrubber {
	mockScrubber := logmocks.NewMockScrubber(ctrl)
	mockScrubber.EXPECT().Scrub(gomock.Any()).AnyTimes().DoAndReturn(func(logBytes []byte) []byte {
		return logBytes
	})

	return mockScrubber
}

func newContainerItem(name string) *service.ContainerItem {
	return &service.ContainerItem{
		Name: &name,
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

func newBlob(blobName, containerName string) storage.Blob {
	return storage.Blob{
		Name:      getBlobName(blobName),
		Container: storage.Container{Name: containerName},
	}
}

func newBlobItem(name string, contentLength int64, blobTime time.Time) *container.BlobItem {
	blobName := getBlobName(name)
	return &container.BlobItem{
		Name: &blobName,
		Properties: &container.BlobProperties{
			ContentLength: &contentLength,
			CreationTime:  &blobTime,
		},
	}
}

func getListBlobsFlatResponse(containers []*container.BlobItem) azblob.ListBlobsFlatResponse {
	if len(containers) == 0 {
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

func getDatadogConfig(getDatadogLogResp func(req *http.Request) (*http.Response, error)) (*datadog.Configuration, chan datadogV2.HTTPLogItem) {
	submittedLogsChan := make(chan datadogV2.HTTPLogItem)
	// Use the CustomRoundTripper with a standard http.Transport
	customRoundTripper := &CustomRoundTripper{
		transport: &http.Transport{},
		getResponse: func(req *http.Request) (*http.Response, error) {
			if req == nil {
				return nil, errors.New("request is nil")
			}

			// body is gzipped, so we need to decompress it
			r, err := gzip.NewReader(req.Body)
			if err != nil {
				return nil, err
			}

			// read the decompressed body
			bodyBytes, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}

			var currLogs []datadogV2.HTTPLogItem
			err = json.Unmarshal(bodyBytes, &currLogs)
			if err != nil {
				return nil, err
			}
			// Send the logs to the channel
			for _, logItem := range currLogs {
				submittedLogsChan <- logItem
			}
			return getDatadogLogResp(req)
		},
	}

	datadogConfig := datadog.NewConfiguration()
	datadogConfig.HTTPClient = &http.Client{
		Transport: customRoundTripper,
	}
	return datadogConfig, submittedLogsChan
}

func mockedRun(t *testing.T, containers []*service.ContainerItem, blobs []*container.BlobItem, getDownloadResp func(*azblob.DownloadStreamOptions) azblob.DownloadStreamResponse, cursorResp azblob.DownloadStreamResponse, deadletterqueueResp azblob.DownloadStreamResponse, uploadFunc func(context.Context, string, string, []byte, *azblob.UploadBufferOptions) (azblob.UploadBufferResponse, error), getDatadogLogResp func(req *http.Request) (*http.Response, error)) ([]datadogV2.HTTPLogItem, error) {
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
		if blobName == deadletterqueue.BlobName {
			return deadletterqueueResp, nil
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
	mockClient.EXPECT().CreateContainer(gomock.Any(), storage.ForwarderContainer, gomock.Any()).Return(resp, nil).AnyTimes()

	mockPiiScrubber := newMockPiiScrubber(ctrl)

	ctx := context.Background()

	datadogConfig, logsChan := getDatadogConfig(getDatadogLogResp)
	submittedLogs := make([]datadogV2.HTTPLogItem, 0)
	submittedLogsGroup, ctx := errgroup.WithContext(ctx)
	submittedLogsGroup.Go(func() error {
		for logItem := range logsChan {
			submittedLogs = append(submittedLogs, logItem)
		}
		return nil
	})

	runErr, _ := run(ctx, nullLogger(), 1, datadogConfig, mockClient, mockPiiScrubber, time.Now, versionTag)
	close(logsChan)
	logsErr := submittedLogsGroup.Wait()
	return submittedLogs, errors.Join(runErr, logsErr)
}

func TestRun(t *testing.T) {
	t.Parallel()

	t.Run("execute the basic functionality", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testString := "test"
		validLog := getLogWithContent(testString, 5*time.Minute)
		expectedBytesForLog := len(validLog) + 1 // +1 for newline

		containerName := "insights-logs-functionapplogs"

		containerPage := []*service.ContainerItem{
			newContainerItem(containerName),
		}
		expiredBlob := newBlobItem("expired", int64(expectedBytesForLog), time.Now().Add(storage.LookBackPeriod*2))
		blobPage := []*container.BlobItem{
			newBlobItem("testA", int64(expectedBytesForLog), time.Now()),
			newBlobItem("testB", int64(expectedBytesForLog), time.Now()),
			expiredBlob,
		}

		getDownloadResp := func(o *azblob.DownloadStreamOptions) azblob.DownloadStreamResponse {
			resp := azblob.DownloadStreamResponse{}
			resp.Body = io.NopCloser(strings.NewReader(string(validLog)))
			return resp
		}

		cursorsWithExpiredBlob := cursor.New(nil)
		cursorsWithExpiredBlob.Set(containerName, *expiredBlob.Name, 300)
		cursorBytes, err := cursorsWithExpiredBlob.JSONBytes()
		require.NoError(t, err)

		cursorResp := azblob.DownloadStreamResponse{}
		cursorResp.Body = io.NopCloser(strings.NewReader(string(cursorBytes)))

		deadLetterQueue := deadletterqueue.DeadLetterQueue{}
		deadLetterQueueBytes, err := deadLetterQueue.JSONBytes()
		require.NoError(t, err)

		deadLetterQueueResp := azblob.DownloadStreamResponse{}
		deadLetterQueueResp.Body = io.NopCloser(strings.NewReader(string(deadLetterQueueBytes)))

		var uploadedMetrics []byte
		var finalCursors *cursor.Cursors
		uploadFunc := func(ctx context.Context, containerName string, blobName string, content []byte, o *azblob.UploadBufferOptions) (azblob.UploadBufferResponse, error) {
			if strings.Contains(blobName, "metrics_") {
				uploadedMetrics = append(uploadedMetrics, content...)
			}
			if strings.Contains(blobName, "cursors") {
				finalCursors = cursor.FromBytes(content, log.NewEntry(nullLogger()))
				require.NoError(t, err)
			}
			return azblob.UploadBufferResponse{}, nil
		}
		var latestHeaders http.Header
		logResp := func(req *http.Request) (*http.Response, error) {
			if req == nil {
				return nil, errors.New("request is nil")
			}
			latestHeaders = req.Header
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		}

		// WHEN
		submittedLogs, err := mockedRun(t, containerPage, blobPage, getDownloadResp, cursorResp, deadLetterQueueResp, uploadFunc, logResp)

		// THEN
		assert.NoError(t, err)

		finalMetrics, err := metrics.FromBytes(uploadedMetrics)
		assert.NoError(t, err)
		totalLoad := 0
		totalBytes := 0
		for _, metric := range finalMetrics {
			assert.Equal(t, versionTag, metric.Version)
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

		assert.Equal(t, int64(0), finalCursors.Get(containerName, *expiredBlob.Name))
		for _, logItem := range submittedLogs {
			assert.Equal(t, azureService, *logItem.Service)
			assert.Equal(t, "azure.web", *logItem.Ddsource)
			assert.Contains(t, *logItem.Ddtags, "forwarder:lfo")
		}

		assert.Equal(t, "gzip", latestHeaders.Get("Content-Encoding"))
		assert.Equal(t, "lfo", latestHeaders.Get("dd_evp_origin"))
	})

	t.Run("continues processing on errors", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testString := "test"
		validLog := getLogWithContent(testString, 5*time.Minute)
		expectedBytesForLog := len(validLog) + 1 // +1 for newline

		containerPage := []*service.ContainerItem{
			newContainerItem("insights-logs-functionapplogs"),
		}
		blobPage := []*container.BlobItem{
			newBlobItem("testA", int64(expectedBytesForLog), time.Now()),
			newBlobItem("testB", int64(expectedBytesForLog), time.Now()),
		}

		firstBlob := true

		getDownloadResp := func(o *azblob.DownloadStreamOptions) azblob.DownloadStreamResponse {
			resp := azblob.DownloadStreamResponse{}
			if firstBlob {
				firstBlob = false
				resp.Body = io.NopCloser(strings.NewReader("{\"test\": \"invalid..."))
			} else {
				resp.Body = io.NopCloser(strings.NewReader(string(validLog)))
			}
			return resp
		}

		cursorResp := azblob.DownloadStreamResponse{}
		cursorResp.Body = io.NopCloser(strings.NewReader("{}"))

		deadLetterQueueResp := azblob.DownloadStreamResponse{}
		deadLetterQueueResp.Body = io.NopCloser(strings.NewReader("[]"))

		var uploadedMetrics []byte
		uploadFunc := func(ctx context.Context, containerName string, blobName string, content []byte, o *azblob.UploadBufferOptions) (azblob.UploadBufferResponse, error) {
			if strings.Contains(blobName, "metrics_") {
				uploadedMetrics = append(uploadedMetrics, content...)
			}
			return azblob.UploadBufferResponse{}, nil
		}
		logResp := func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		}

		// WHEN
		submittedLogs, err := mockedRun(t, containerPage, blobPage, getDownloadResp, cursorResp, deadLetterQueueResp, uploadFunc, logResp)

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
		assert.Equal(t, len(blobPage)-1, totalLoad)
		assert.Equal(t, (len(blobPage)-1)*(expectedBytesForLog), totalBytes)
		assert.Len(t, submittedLogs, len(blobPage)-1)

		for _, logItem := range submittedLogs {
			assert.Equal(t, azureService, *logItem.Service)
			assert.Equal(t, "azure.web", *logItem.Ddsource)
			assert.Contains(t, *logItem.Ddtags, "forwarder:lfo")
		}
	})
}

func TestProcessLogs(t *testing.T) {
	t.Parallel()

	t.Run("submits logs to dd", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		validLog := "{ \"time\": \"" + azureTimestamp(time.Now().Add(-5*time.Minute)) + "\", \"resourceId\": \"" + resourceId + "\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':'','roleInstance':'BD28A314-638598491096328853','message':'LoggerFilterOptions\\n{\\n  \\'MinLevel\\': \\'None\\',\\n  \\'Rules\\': [\\n    {\\n      \\'ProviderName\\': null,\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'None\\',\\n      \\'Filter\\': null\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Logging.ApplicationInsights.ApplicationInsightsLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'Trace\\',\\n      \\'Filter\\': null\\n    }\\n  ]\\n}','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}\n"
		var content string
		for range 3 {
			content += validLog
		}
		reader := io.NopCloser(strings.NewReader(content))

		ctrl := gomock.NewController(t)
		var submittedLogs []datadogV2.HTTPLogItem
		mockDDClient := logmocks.NewMockDatadogLogsSubmitter(ctrl)
		mockDDClient.EXPECT().SubmitLog(gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(2).DoAndReturn(func(ctx context.Context, body []datadogV2.HTTPLogItem, o ...datadogV2.SubmitLogOptionalParameters) (any, *http.Response, error) {
			submittedLogs = append(submittedLogs, body...)
			return nil, nil, nil
		})

		datadogClient := logs.NewClient(mockDDClient)
		defer datadogClient.Flush(context.Background())

		eg, egCtx := errgroup.WithContext(context.Background())

		logsCh := make(chan *logs.Log, 100)
		volumeCh := make(chan string, 100)
		bytesCh := make(chan resourceBytes, 100)

		// WHEN
		eg.Go(func() error {
			defer close(volumeCh)
			defer close(bytesCh)
			return processLogs(egCtx, datadogClient, time.Now, log.NewEntry(nullLogger()), logsCh, volumeCh, bytesCh)
		})
		eg.Go(func() error {
			defer close(logsCh)
			_, _, err := parseLogs(reader, newBlob(resourceId, "insights-logs-functionapplogs"), newMockPiiScrubber(ctrl), logsCh)
			return err
		})
		err := eg.Wait()

		// THEN
		assert.NoError(t, err)
		assert.Len(t, submittedLogs, 3)
		for _, logItem := range submittedLogs {
			assert.Equal(t, azureService, *logItem.Service)
			assert.Equal(t, "azure.web", *logItem.Ddsource)
			assert.Contains(t, *logItem.Ddtags, "forwarder:lfo")
		}
	})

	t.Run("too large logs are not submitted", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		oneHundredAs := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		var content string
		for range logs.MaxLogSize / 100 {
			content += oneHundredAs
		}
		invalidLog := getLogWithContent(content, 5*time.Minute)
		reader := io.NopCloser(strings.NewReader(string(invalidLog)))

		ctrl := gomock.NewController(t)
		mockDDClient := logmocks.NewMockDatadogLogsSubmitter(ctrl)

		datadogClient := logs.NewClient(mockDDClient)
		defer datadogClient.Flush(context.Background())

		eg, egCtx := errgroup.WithContext(context.Background())

		logsCh := make(chan *logs.Log, 100)
		volumeCh := make(chan string, 100)
		bytesCh := make(chan resourceBytes, 100)

		containerName := "insights-logs-functionapplogs"

		// WHEN
		eg.Go(func() error {
			defer close(volumeCh)
			defer close(bytesCh)
			return processLogs(egCtx, datadogClient, time.Now, log.NewEntry(nullLogger()), logsCh, volumeCh, bytesCh)
		})
		eg.Go(func() error {
			defer close(logsCh)
			_, _, err := parseLogs(reader, newBlob(resourceId, containerName), newMockPiiScrubber(ctrl), logsCh)
			return err
		})

		err := eg.Wait()

		// THEN
		assert.Nil(t, err)
		mockDDClient.EXPECT().SubmitLog(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	})

	t.Run("too old logs are not submitted", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		invalidLog := getLogWithContent("old man yells at cloud", 19*time.Hour)
		reader := io.NopCloser(strings.NewReader(string(invalidLog)))

		ctrl := gomock.NewController(t)
		mockDDClient := logmocks.NewMockDatadogLogsSubmitter(ctrl)

		datadogClient := logs.NewClient(mockDDClient)
		defer datadogClient.Flush(context.Background())

		eg, egCtx := errgroup.WithContext(context.Background())

		logsCh := make(chan *logs.Log, 100)
		volumeCh := make(chan string, 100)
		bytesCh := make(chan resourceBytes, 100)

		containerName := "insights-logs-functionapplogs"

		// WHEN
		eg.Go(func() error {
			defer close(volumeCh)
			defer close(bytesCh)
			return processLogs(egCtx, datadogClient, time.Now, log.NewEntry(nullLogger()), logsCh, volumeCh, bytesCh)
		})
		eg.Go(func() error {
			defer close(logsCh)
			_, _, err := parseLogs(reader, newBlob(resourceId, containerName), newMockPiiScrubber(ctrl), logsCh)
			return err
		})

		err := eg.Wait()

		// THEN
		assert.Nil(t, err)
		mockDDClient.EXPECT().SubmitLog(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	})
}

func TestParseLogs(t *testing.T) {
	t.Parallel()

	t.Run("creates a Log from raw log", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		ctrl := gomock.NewController(t)
		validLog := getLogWithContent("test", 5*time.Minute)
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
			_, _, err := parseLogs(reader, newBlob(resourceId, "insights-logs-functionapplogs"), newMockPiiScrubber(ctrl), logsChannel)
			return err
		})
		err := eg.Wait()

		// THEN
		assert.NoError(t, err)
		assert.Len(t, got, 3)
	})
}

var (
	//go:embed fixtures/aks_logs.json
	aksLogData string

	//go:embed fixtures/function_app_logs.json
	functionAppLogData string

	//go:embed fixtures/activedirectory/audit_logs.json
	adAuditLogData string

	//go:embed fixtures/activedirectory/managed_identity_sign_in_logs.json
	adManagedIdentitySignInLogData string

	//go:embed fixtures/activedirectory/ms_graph_activity_logs.json
	adMicrosoftGraphActivityLogData string

	//go:embed fixtures/activedirectory/non_interactive_user_sign_in_logs.json
	adNonInteractiveUserSignInLogData string

	//go:embed fixtures/activedirectory/risky_users_logs.json
	adRiskyUsersLogData string

	//go:embed fixtures/activedirectory/service_principal_sign_in_logs.json
	adServicePrincipalSignInLogData string

	//go:embed fixtures/activedirectory/sign_in_logs.json
	adSignInLogData string

	//go:embed fixtures/activedirectory/user_risk_event_logs.json
	adUserRiskEventLogData string
)

func TestCursors(t *testing.T) {
	t.Parallel()

	t.Run("works with aks logs", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		containerName := "insights-logs-kube-audit"
		blobName := "aks_logs.json"

		containerPage := []*service.ContainerItem{
			newContainerItem(containerName),
		}

		n := 5 // Number of times to execute

		var currentLogData []byte
		now := time.Now()

		lastCursor := cursor.New(nil)

		for i := 0; i < n; i++ {
			// REPEATED GIVEN
			currentLogData = append(currentLogData, aksLogData...)
			currentLength := int64(len(currentLogData))

			blobItem := &container.BlobItem{
				Name: &blobName,
				Properties: &container.BlobProperties{
					ContentLength: &currentLength,
					CreationTime:  &now,
				},
			}

			cursorResp := azblob.DownloadStreamResponse{}
			cursorResp.Body = io.NopCloser(strings.NewReader("{}"))

			deadLetterQeueResp := azblob.DownloadStreamResponse{}
			deadLetterQeueResp.Body = io.NopCloser(strings.NewReader("[]"))

			uploadFunc := func(ctx context.Context, containerName string, blobName string, content []byte, o *azblob.UploadBufferOptions) (azblob.UploadBufferResponse, error) {
				if blobName == cursor.BlobName {
					lastCursor = cursor.FromBytes(content, log.NewEntry(nullLogger()))
				}
				return azblob.UploadBufferResponse{}, nil
			}

			getDownloadResp := func(o *azblob.DownloadStreamOptions) azblob.DownloadStreamResponse {
				resp := azblob.DownloadStreamResponse{}
				resp.Body = io.NopCloser(strings.NewReader(string(currentLogData[o.Range.Offset:])))
				return resp
			}

			logResp := func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("")),
				}, nil
			}

			// WHEN
			submittedLogs, err := mockedRun(t, containerPage, []*container.BlobItem{blobItem}, getDownloadResp, cursorResp, deadLetterQeueResp, uploadFunc, logResp)

			// THEN
			assert.NoError(t, err)

			assert.Equal(t, int64(len(currentLogData)), lastCursor.Get(containerName, blobName))

			for _, logItem := range submittedLogs {
				assert.Equal(t, azureService, *logItem.Service)
				assert.Equal(t, "azure.web.sites", *logItem.Ddsource)
				assert.Contains(t, *logItem.Ddtags, "forwarder:lfo")
			}
		}
	})

	t.Run("works with function app logs", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		containerName := "insights-logs-functionapplogs"
		blobName := "function_app_logs.json"

		containerPage := []*service.ContainerItem{
			newContainerItem(containerName),
		}

		n := 5 // Number of times to execute

		var currentLogData []byte
		now := time.Now()

		lastCursor := cursor.New(nil)

		for i := 0; i < n; i++ {
			// REPEATED GIVEN
			currentLogData = append(currentLogData, functionAppLogData...)
			currentLength := int64(len(currentLogData))

			blobItem := &container.BlobItem{
				Name: &blobName,
				Properties: &container.BlobProperties{
					ContentLength: &currentLength,
					CreationTime:  &now,
				},
			}

			cursorResp := azblob.DownloadStreamResponse{}
			cursorResp.Body = io.NopCloser(strings.NewReader("{}"))

			deadLetterQueueResp := azblob.DownloadStreamResponse{}
			deadLetterQueueResp.Body = io.NopCloser(strings.NewReader("[]"))

			uploadFunc := func(ctx context.Context, containerName string, blobName string, content []byte, o *azblob.UploadBufferOptions) (azblob.UploadBufferResponse, error) {
				if blobName == cursor.BlobName {
					lastCursor = cursor.FromBytes(content, log.NewEntry(nullLogger()))
				}
				return azblob.UploadBufferResponse{}, nil
			}

			getDownloadResp := func(o *azblob.DownloadStreamOptions) azblob.DownloadStreamResponse {
				resp := azblob.DownloadStreamResponse{}
				resp.Body = io.NopCloser(strings.NewReader(string(currentLogData[o.Range.Offset:])))
				return resp
			}
			logResp := func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("")),
				}, nil
			}

			// WHEN
			submittedLogs, err := mockedRun(t, containerPage, []*container.BlobItem{blobItem}, getDownloadResp, cursorResp, deadLetterQueueResp, uploadFunc, logResp)

			// THEN
			assert.NoError(t, err)

			assert.Equal(t, int64(len(currentLogData)), lastCursor.Get(containerName, blobName))

			for _, logItem := range submittedLogs {
				assert.Equal(t, azureService, *logItem.Service)
				assert.Equal(t, "azure.web.sites", *logItem.Ddsource)
				assert.Contains(t, *logItem.Ddtags, "forwarder:lfo")
			}
		}
	})
}

func TestCursorsOnActiveDirectoryLogs(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		containerName string
		testFileName  string
		testLogData   string
	}{
		"works with AD audit logs": {
			containerName: "insights-logs-auditlogs",
			testFileName:  "audit_logs.json",
			testLogData:   adAuditLogData,
		},
		"works with AD managed identity sign in logs": {
			containerName: "insights-logs-managedidentitysigninlogs",
			testFileName:  "managed_identity_sign_in_logs.json",
			testLogData:   adManagedIdentitySignInLogData,
		},
		"works with AD microsoft graph activity logs": {
			containerName: "insights-logs-microsoftgraphactivitylogs",
			testFileName:  "ms_graph_activity_logs.json",
			testLogData:   adMicrosoftGraphActivityLogData,
		},
		"works with AD non interactive user sign in logs": {
			containerName: "insights-logs-noninteractiveusersigninlogs",
			testFileName:  "non_interactive_user_sign_in_logs.json",
			testLogData:   adNonInteractiveUserSignInLogData,
		},
		"works with AD risky users logs": {
			containerName: "insights-logs-riskyusers",
			testFileName:  "risky_users_logs.json",
			testLogData:   adRiskyUsersLogData,
		},
		"works with AD service principal sign in logs": {
			containerName: "insights-logs-serviceprincipalsigninlogs",
			testFileName:  "service_principal_sign_in_logs.json",
			testLogData:   adServicePrincipalSignInLogData,
		},
		"works with AD sign in logs": {
			containerName: "insights-logs-signinlogs",
			testFileName:  "sign_in_logs.json",
			testLogData:   adSignInLogData,
		},
		"works with AD user risk event logs": {
			containerName: "insights-logs-userriskevents",
			testFileName:  "user_risk_event_logs.json",
			testLogData:   adUserRiskEventLogData,
		},
	}

	for name, test := range tests {
		// GIVEN
		containerName := test.containerName
		blobName := test.testFileName

		containerPage := []*service.ContainerItem{
			newContainerItem(containerName),
		}

		n := 5 // Number of times to execute

		var currentLogData []byte
		now := time.Now()

		lastCursor := cursor.New(nil)

		for i := 0; i < n; i++ {
			// REPEATED GIVEN
			currentLogData = append(currentLogData, test.testLogData...)
			currentLength := int64(len(currentLogData))

			blobItem := &container.BlobItem{
				Name: &blobName,
				Properties: &container.BlobProperties{
					ContentLength: &currentLength,
					CreationTime:  &now,
				},
			}

			cursorResp := azblob.DownloadStreamResponse{}
			cursorResp.Body = io.NopCloser(strings.NewReader("{}"))

			deadLetterQueueResp := azblob.DownloadStreamResponse{}
			deadLetterQueueResp.Body = io.NopCloser(strings.NewReader("[]"))

			uploadFunc := func(ctx context.Context, containerName string, blobName string, content []byte, o *azblob.UploadBufferOptions) (azblob.UploadBufferResponse, error) {
				if blobName == cursor.BlobName {
					lastCursor = cursor.FromBytes(content, log.NewEntry(nullLogger()))
				}
				return azblob.UploadBufferResponse{}, nil
			}

			getDownloadResp := func(o *azblob.DownloadStreamOptions) azblob.DownloadStreamResponse {
				resp := azblob.DownloadStreamResponse{}
				resp.Body = io.NopCloser(strings.NewReader(string(currentLogData[o.Range.Offset:])))
				return resp
			}
			logResp := func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("")),
				}, nil
			}

			// WHEN
			submittedLogs, err := mockedRun(t, containerPage, []*container.BlobItem{blobItem}, getDownloadResp, cursorResp, deadLetterQueueResp, uploadFunc, logResp)

			// THEN
			assert.NoError(t, err)

			assert.Equal(t, int64(len(currentLogData)), lastCursor.Get(containerName, blobName), name)

			for _, logItem := range submittedLogs {
				assert.Equal(t, azureService, *logItem.Service)
				assert.Equal(t, "azure.aadiam", *logItem.Ddsource)
				assert.Contains(t, *logItem.Ddtags, "forwarder:lfo")
			}
		}
	}
}

type FaultyRoundTripper struct {
}

func (f *FaultyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("faulty")
}

func TestProcessDLQ(t *testing.T) {
	t.Parallel()

	t.Run("processes the dead letter queue", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		ctrl := gomock.NewController(t)
		dlq, err := deadletterqueue.FromBytes(nil, []byte("[]"))
		require.NoError(t, err)
		currentTime := time.Now().UTC()
		formattedTime := currentTime.Format(time.RFC3339)
		logItem := datadogV2.HTTPLogItem{
			Message:              fmt.Sprintf("{\"time\":\"%s\"}", formattedTime),
			AdditionalProperties: map[string]any{"time": formattedTime},
		}
		queue := []datadogV2.HTTPLogItem{logItem}
		dlq.Add(queue)
		data, err := dlq.JSONBytes()
		require.NoError(t, err)
		reader := io.NopCloser(bytes.NewReader(data))
		response := azblob.DownloadStreamResponse{
			DownloadResponse: blob.DownloadResponse{
				Body: reader,
			},
		}
		mockClient := storagemocks.NewMockAzureBlobClient(ctrl)
		mockClient.EXPECT().DownloadStream(gomock.Any(), storage.ForwarderContainer, deadletterqueue.BlobName, nil).Return(response, nil)
		createContainerResponse := azblob.CreateContainerResponse{}
		mockClient.EXPECT().CreateContainer(gomock.Any(), storage.ForwarderContainer, nil).AnyTimes().Return(createContainerResponse, nil)

		// Add expectation for UploadBuffer call
		uploadResponse := azblob.UploadBufferResponse{}
		mockClient.EXPECT().UploadBuffer(gomock.Any(), storage.ForwarderContainer, deadletterqueue.BlobName, gomock.Any(), gomock.Any()).Return(uploadResponse, nil)

		storageClient := storage.NewClient(mockClient)

		datadogClient := logmocks.NewMockDatadogLogsSubmitter(ctrl)
		datadogClient.EXPECT().SubmitLog(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil, nil)
		logsClient := logs.NewClient(datadogClient)

		processedDatadogClient := logmocks.NewMockDatadogLogsSubmitter(ctrl)
		processedLogsClient := logs.NewClient(processedDatadogClient)
		processedClients := []*logs.Client{processedLogsClient}

		ctx := context.Background()

		// WHEN
		err = processDeadLetterQueue(ctx, time.Now, log.NewEntry(nullLogger()), storageClient, logsClient, processedClients)

		// THEN
		assert.NoError(t, err)
	})
}

// TestRunMain exists for performance testing purposes.
func TestRunMain(t *testing.T) {
	t.Parallel()

	t.Run("fetchAndProcessLogs main", func(t *testing.T) {
		t.Parallel()
		if os.Getenv("FORWARDER_PROFILING") != "true" {
			t.Skip("Skipping profiling tests as FORWARDER_PROFILING is not set to true")
		}
		main()
	})
}

// getAzuriteConnectionString returns the connection string for a given Azurite container.
func getAzuriteConnectionString(ctx context.Context, container testcontainers.Container) (string, error) {
	ports, err := container.Ports(ctx)
	if err != nil {
		return "", err
	}

	// map exposed ports to host ports
	portMapping := make(map[nat.Port]string)
	for port, bindings := range ports {
		for _, binding := range bindings {
			portMapping[port] = binding.HostPort
		}
	}

	blobEndpoint := portMapping["10000/tcp"]
	queueEndpoint := portMapping["10001/tcp"]
	tableEndpoint := portMapping["10002/tcp"]

	// Construct the connection string
	return fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:%s/devstoreaccount1;QueueEndpoint=http://127.0.0.1:%s/devstoreaccount1;TableEndpoint=http://127.0.0.1:%s/devstoreaccount1;",
		blobEndpoint, queueEndpoint, tableEndpoint,
	), nil

}

func azuriteRun(t *testing.T, ctx context.Context, azBlobClient storage.AzureBlobClient, now customtime.Now) ([]datadogV2.HTTPLogItem, error, map[string]error) {
	datadogConfig, logsChan := getDatadogConfig(func(req *http.Request) (*http.Response, error) {
		if req == nil {
			return nil, errors.New("request is nil")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	})

	submittedLogs := make([]datadogV2.HTTPLogItem, 0)
	submittedLogsGroup, ctx := errgroup.WithContext(ctx)

	ctrl := gomock.NewController(t)
	mockPiiScrubber := newMockPiiScrubber(ctrl)

	submittedLogsGroup.Go(func() error {
		for logItem := range logsChan {
			submittedLogs = append(submittedLogs, logItem)
		}
		return nil
	})

	runErr, blobErrors := run(ctx, nullLogger(), 1, datadogConfig, azBlobClient, mockPiiScrubber, now, versionTag)

	close(logsChan)
	logsErr := submittedLogsGroup.Wait()

	return submittedLogs, errors.Join(runErr, logsErr), blobErrors
}

var (
	//go:embed fixtures/cursor_validation_state_a.json
	blobStateA []byte

	//go:embed fixtures/cursor_validation_state_b.json
	blobStateB []byte
)

// TestRunWithAzurite exists for performance testing purposes.
func TestRunWithAzurite(t *testing.T) {
	t.Parallel()

	t.Run("run two stage test against run", func(t *testing.T) {
		t.Parallel()
		if os.Getenv("RUN_AZURITE_TESTS") != "true" {
			t.Skip("Skipping azurite tests as RUN_AZURITE_TESTS is not set to true")
		}
		ctx := context.Background()

		// use testcontainers to create an azurite container
		// azurite is a storage account emulator
		req := testcontainers.ContainerRequest{
			Image:        "mcr.microsoft.com/azure-storage/azurite",
			ExposedPorts: []string{"10000/tcp", "10001/tcp", "10002/tcp"},
			WaitingFor:   wait.ForLog("Azurite Blob service is successfully listening at http://0.0.0.0:10000"),
		}
		azurite, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})

		connectionString, err := getAzuriteConnectionString(ctx, azurite)
		require.NoError(t, err)

		azBlobClient, err := azblob.NewClientFromConnectionString(connectionString, nil)
		require.NoError(t, err)

		functionAppContainer := "insights-logs-functionapplogs"

		// create azure storage container needed to upload the blobs
		_, err = azBlobClient.CreateContainer(ctx, functionAppContainer, nil)
		require.NoError(t, err)

		blobName := "resourceId=/SUBSCRIPTIONS/34464906-34FE-401E-A420-79BD0CE2A1DA/RESOURCEGROUPS/LOGGY-EASTUS2_GROUP/PROVIDERS/MICROSOFT.WEB/SITES/LOGGY-EASTUS2/y=2025/m=04/d=17/h=20/m=00/PT1H.json"

		_, err = azBlobClient.UploadBuffer(ctx, functionAppContainer, blobName, blobStateA, nil)
		require.NoError(t, err)

		// mock now
		customNow := func() time.Time {
			// return the time object for timestamp (UTC): 2025-04-21T17:30:25Z
			return time.Date(2025, 4, 21, 18, 30, 0, 0, time.UTC)
		}

		// Do run A
		_, err, _ = azuriteRun(t, ctx, azBlobClient, customNow)
		require.NoError(t, err)

		// Upload the second state
		_, err = azBlobClient.UploadBuffer(ctx, functionAppContainer, blobName, blobStateB, nil)
		require.NoError(t, err)

		// Do run B
		_, err, _ = azuriteRun(t, ctx, azBlobClient, customNow)
		require.NoError(t, err)

		testcontainers.CleanupContainer(t, azurite)
		require.NoError(t, err)
	})
}
