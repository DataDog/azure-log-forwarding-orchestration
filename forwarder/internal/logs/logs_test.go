package logs_test

import (
	// stdlib
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	// 3p
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/environment"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs/mocks"
)

func azureTimestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

const resourceId string = "/subscriptions/0b62a232-b8db-4380-9da6-640f7272ed6d/resourceGroups/forwarder-integration-testing/providers/Microsoft.Web/sites/forwarderintegrationtesting"

func getLogWithContent(content string, delay time.Duration) []byte {
	timestamp := time.Now().Add(-delay)
	return []byte("{ \"time\": \"" + azureTimestamp(timestamp) + "\", \"resourceId\": \"/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':'','roleInstance':'BD28A314-638598491096328853','message':'" + content + "','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}")
}

const functionAppContainer = "insights-logs-functionapplogs"
const controlPlaneId = "9b008b0cc1ab"
const configId = "8e0ce1e1e048"

func MockLogger() (*log.Entry, *bytes.Buffer) {
	var output []byte
	buffer := bytes.NewBuffer(output)
	logger := log.New()
	logger.SetOutput(buffer)
	return log.NewEntry(logger), buffer
}

func MockScrubber(t *testing.T, scrubbedLog []byte) *mocks.MockScrubber {
	ctrl := gomock.NewController(t)
	mockScrubber := mocks.NewMockScrubber(ctrl)
	mockScrubber.EXPECT().Scrub(gomock.Any()).Return(&scrubbedLog).AnyTimes()
	return mockScrubber
}

func TestMain(m *testing.M) {
	os.Setenv(environment.CONTROL_PLANE_ID, controlPlaneId)
	os.Setenv(environment.CONFIG_ID, configId)

	code := m.Run()

	os.Unsetenv(environment.CONTROL_PLANE_ID)
	os.Unsetenv(environment.CONFIG_ID)

	os.Exit(code)
}

func TestAddLog(t *testing.T) {
	t.Parallel()

	t.Run("batches when over max payload size", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		var payload []*logs.Log
		prefix := "{\"category\":\"a\",\"resourceId\":\"/subscriptions/0b62a232-b8db-4380-9da6-640f7272ed6d/resourceGroups/lfo-qa/providers/Microsoft.Web/sites/loggya/appServices\",\"key\":\""
		suffix := "\"}"
		targetSize := logs.MaxLogSize/2 - len(prefix) - len(suffix) - 3
		logString := fmt.Sprintf("%s%s%s", prefix, strings.Repeat("a", targetSize), suffix)
		logBytes := []byte(logString)
		for range 12 {
			currLog, err := logs.NewLog(logBytes, functionAppContainer, resourceId, MockScrubber(t, logBytes))
			currLog.Time = time.Now().Add(-5 * time.Minute)
			require.NoError(t, err)
			payload = append(payload, currLog)
		}

		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockDatadogLogsSubmitter(ctrl)
		mockClient.EXPECT().SubmitLog(gomock.Any(), gomock.Any(), gomock.Any()).Times(2)

		client := logs.NewClient(mockClient)
		ctx := context.Background()
		var err error
		logger, buffer := MockLogger()

		// WHEN
		for _, l := range payload {
			errors.Join(client.AddLog(ctx, logger, l), err)
		}
		errors.Join(client.Flush(ctx), err)

		// THEN
		assert.NoError(t, err)
		assert.Empty(t, buffer)
	})
}

func assertTags(t *testing.T, log *logs.Log) {
	assert.Contains(t, log.Tags, "forwarder:lfo")
	assert.Contains(t, log.Tags, "subscription_id:0B62A232-B8DB-4380-9DA6-640F7272ED6D")
	assert.Contains(t, log.Tags, "source:azure.web.sites")
	assert.Contains(t, log.Tags, "resource_group:FORWARDER-INTEGRATION-TESTING")
	assert.Contains(t, log.Tags, fmt.Sprintf("control_plane_id:%s", controlPlaneId))
	assert.Contains(t, log.Tags, fmt.Sprintf("config_id:%s", configId))
	assert.Contains(t, log.Source, "azure.web.sites")
	assert.Contains(t, log.Service, logs.AzureService)
}

var validLog = []byte("{ \"time\": \"2024-08-21T15:12:24Z\", \"resourceId\": \"/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':'','roleInstance':'BD28A314-638598491096328853','message':'LoggerFilterOptions\\n{\\n  \\'MinLevel\\': \\'None\\',\\n  \\'Rules\\': [\\n    {\\n      \\'ProviderName\\': null,\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'None\\',\\n      \\'Filter\\': null\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Logging.ApplicationInsights.ApplicationInsightsLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'Trace\\',\\n      \\'Filter\\': null\\n    }\\n  ]\\n}','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}")

func TestNewLog(t *testing.T) {
	t.Parallel()

	t.Run("creates a Log from raw log", func(t *testing.T) {
		t.Parallel()
		// WHEN
		log, err := logs.NewLog(validLog, functionAppContainer, resourceId, MockScrubber(t, validLog))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING", strings.ToUpper(log.ResourceId))
		assert.Equal(t, "FunctionAppLogs", log.Category)
		assertTags(t, log)
		assert.NotNil(t, log)

	})

	t.Run("handles an array of strings", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		var validLog = []byte("{ \"time\": \"2024-08-21T15:12:24Z\", \"resourceId\": \"/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':['app1', 'app2'],'roleInstance':'BD28A314-638598491096328853','message':'LoggerFilterOptions\\n{\\n  \\'MinLevel\\': \\'None\\',\\n  \\'Rules\\': [\\n    {\\n      \\'ProviderName\\': null,\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'None\\',\\n      \\'Filter\\': null\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Logging.ApplicationInsights.ApplicationInsightsLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'Trace\\',\\n      \\'Filter\\': null\\n    }\\n  ]\\n}','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}")

		// WHEN
		log, err := logs.NewLog(validLog, functionAppContainer, resourceId, MockScrubber(t, validLog))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING", strings.ToUpper(log.ResourceId))
		assert.Equal(t, "FunctionAppLogs", log.Category)
		assertTags(t, log)
		assert.NotNil(t, log)

	})

	t.Run("handles an array of objects", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		var validLog = []byte("{ \"time\": \"2024-08-21T15:12:24Z\", \"resourceId\": \"/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':[{'app1': null, 'app2': true}, {'app3': 3.0}],'roleInstance':'BD28A314-638598491096328853','message':'LoggerFilterOptions\\n{\\n  \\'MinLevel\\': \\'None\\',\\n  \\'Rules\\': [\\n    {\\n      \\'ProviderName\\': null,\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'None\\',\\n      \\'Filter\\': null\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Logging.ApplicationInsights.ApplicationInsightsLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'Trace\\',\\n      \\'Filter\\': null\\n    }\\n  ]\\n}','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}")

		// WHEN
		log, err := logs.NewLog(validLog, functionAppContainer, resourceId, MockScrubber(t, validLog))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING", strings.ToUpper(log.ResourceId))
		assert.Equal(t, "FunctionAppLogs", log.Category)
		assertTags(t, log)
		assert.NotNil(t, log)

	})

	t.Run("applies correct tags", func(t *testing.T) {
		t.Parallel()

		// WHEN
		log, err := logs.NewLog(validLog, functionAppContainer, resourceId, MockScrubber(t, validLog))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING", strings.ToUpper(log.ResourceId))
		assert.Equal(t, "FunctionAppLogs", log.Category)
		assertTags(t, log)
		assert.NotNil(t, log)
	})

	var incompleteJsonLog = []byte("{ \"time\": \"2024-08-21T15:12:24Z\", ")

	t.Run("returns custom error on incomplete json for standard logs", func(t *testing.T) {
		t.Parallel()
		// WHEN
		log, err := logs.NewLog([]byte("{ \"time\": \"2024-08-21T15:12:24Z\", "), "something normal", resourceId, MockScrubber(t, incompleteJsonLog))

		// THEN
		assert.Error(t, err)
		assert.ErrorIs(t, err, logs.ErrIncompleteLogFile)
		assert.Nil(t, log)
	})

	t.Run("returns custom error on incomplete json for function apps", func(t *testing.T) {
		t.Parallel()
		// WHEN
		log, err := logs.NewLog([]byte("{ \"time\": \"2024-08-21T15:12:24Z\", "), functionAppContainer, resourceId, MockScrubber(t, incompleteJsonLog))

		// THEN
		assert.Error(t, err)
		assert.ErrorIs(t, err, logs.ErrUnexpectedToken)
		assert.Nil(t, log)
	})

	var invalidResourceIdLog = []byte("{ \"resourceId\": \"something\"}")

	t.Run("uses resource id from blob on invalid resource id", func(t *testing.T) {
		t.Parallel()
		// WHEN
		log, err := logs.NewLog(invalidResourceIdLog, "something normal", resourceId, MockScrubber(t, invalidResourceIdLog))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, resourceId, log.ResourceId)
	})

	t.Run("uses resource id from blob on invalid resource id for function apps", func(t *testing.T) {
		t.Parallel()
		// WHEN
		log, err := logs.NewLog(invalidResourceIdLog, functionAppContainer, resourceId, MockScrubber(t, invalidResourceIdLog))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, resourceId, log.ResourceId)
	})

	var plaintextLog = []byte("[2024-08-21T15:12:24] This is a plaintext log")

	t.Run("Creates a valid log for plaintext logs outside of function app logs", func(t *testing.T) {
		t.Parallel()
		log, err := logs.NewLog(plaintextLog, "something normal", resourceId, MockScrubber(t, plaintextLog))
		assert.NoError(t, err)
		assert.NotNil(t, log)
		assert.Equal(t, string(plaintextLog), log.Content())
		assert.Equal(t, resourceId, log.ResourceId)
		assert.Equal(t, "azure.web.sites", log.Source)
		assert.Empty(t, log.Category)
		assert.Equal(t, []string{
			"forwarder:lfo",
			"control_plane_id:9b008b0cc1ab",
			"config_id:8e0ce1e1e048",
			"subscription_id:0b62a232-b8db-4380-9da6-640f7272ed6d",
			"resource_group:forwarder-integration-testing",
			"source:azure.web.sites",
		}, log.Tags)
		assert.Equal(t, logs.AzureService, log.Service)
		assert.Equal(t, log.Level, "Informational")
	})

	t.Run("Creates a valid log for plaintext logs without valid blob resource id", func(t *testing.T) {
		t.Parallel()
		log, err := logs.NewLog(plaintextLog, "something normal", "/some/blob/path", MockScrubber(t, plaintextLog))
		assert.NoError(t, err)
		assert.NotNil(t, log)
		assert.Equal(t, string(plaintextLog), log.Content())
		assert.Empty(t, log.ResourceId)
		assert.Empty(t, log.Category)
		assert.Empty(t, log.Source)
		assert.Equal(t, []string{
			"forwarder:lfo",
			"control_plane_id:9b008b0cc1ab",
			"config_id:8e0ce1e1e048",
		}, log.Tags)
		assert.Equal(t, logs.AzureService, log.Service)
		assert.Equal(t, log.Level, "Informational")
	})
}

func TestValid(t *testing.T) {
	t.Parallel()

	t.Run("valid returns true for a valid log", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		content := getLogWithContent("test", 5*time.Minute)
		l, err := logs.NewLog(content, functionAppContainer, resourceId, MockScrubber(t, []byte(content)))
		require.NoError(t, err)
		logger, buffer := MockLogger()

		// WHEN
		got := l.Validate(logger)

		// THEN
		assert.True(t, got)
		assert.Empty(t, buffer)
	})

	t.Run("valid returns false and warns for an too large log", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		content := getLogWithContent(strings.Repeat("a", logs.MaxPayloadSize), 5*time.Minute)
		l, err := logs.NewLog(content, functionAppContainer, resourceId, MockScrubber(t, []byte(content)))
		require.NoError(t, err)
		logger, buffer := MockLogger()

		// WHEN
		got := l.Validate(logger)

		// THEN
		assert.False(t, got)
		assert.Contains(t, buffer.String(), "Skipping large log")
	})
	t.Run("valid returns false and warns for an too old log", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		content := getLogWithContent("short content", (18*time.Hour)+time.Minute)
		l, err := logs.NewLog(content, functionAppContainer, resourceId, MockScrubber(t, []byte(content)))
		require.NoError(t, err)
		logger, buffer := MockLogger()

		// WHEN
		got := l.Validate(logger)

		// THEN
		assert.False(t, got)
		assert.Contains(t, buffer.String(), "Skipping log older than 18 hours")
	})
}

func TestParseLogs(t *testing.T) {
	t.Parallel()

	t.Run("can parse aks logs", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		workingDir, err := os.Getwd()
		require.NoError(t, err)

		data, err := os.ReadFile(fmt.Sprintf("%s/fixtures/aks_logs.json", workingDir))
		require.NoError(t, err)

		reader := bytes.NewReader(data)
		closer := io.NopCloser(reader)

		var got int

		// WHEN
		for currLog, err := range logs.Parse(closer, "insights-logs-kube-audit", resourceId, MockScrubber(t, data)) {
			require.NoError(t, err)
			require.NotEqual(t, "", currLog.Category)
			require.NotEqual(t, resourceId, currLog.ResourceId)
			require.False(t, currLog.Time.IsZero())
			got += 1
		}

		// THEN
		assert.Equal(t, got, 21)
	})

	t.Run("can parse function app logs", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		workingDir, err := os.Getwd()
		require.NoError(t, err)

		data, err := os.ReadFile(fmt.Sprintf("%s/fixtures/function_app_logs.json", workingDir))
		require.NoError(t, err)

		reader := bytes.NewReader(data)
		closer := io.NopCloser(reader)

		var got int

		// WHEN
		for currLog, err := range logs.Parse(closer, functionAppContainer, resourceId, MockScrubber(t, data)) {
			require.NoError(t, err)
			require.NotEqual(t, "", currLog.Category)
			require.NotEqual(t, resourceId, currLog.ResourceId)
			require.False(t, currLog.Time.IsZero())
			got += 1
		}

		// THEN
		assert.Equal(t, got, 20)
	})

}
