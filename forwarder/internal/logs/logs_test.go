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
	"go.uber.org/mock/gomock"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs/mocks"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)

func azureTimestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

func getLogWithContent(content string, delay time.Duration) []byte {
	timestamp := time.Now().Add(-delay)
	return []byte("{ \"time\": \"" + azureTimestamp(timestamp) + "\", \"resourceId\": \"/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':'','roleInstance':'BD28A314-638598491096328853','message':'" + content + "','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}")
}

const functionAppContainer = "insights-logs-functionapplogs"

func MockLogger() (*log.Entry, *bytes.Buffer) {
	var output []byte
	buffer := bytes.NewBuffer(output)
	logger := log.New()
	logger.SetOutput(buffer)
	return log.NewEntry(logger), buffer
}

func TestAddLog(t *testing.T) {
	t.Parallel()

	t.Run("batches when over max payload size", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		var payload []*logs.Log
		prefix := "{\"category\":\"a\",\"resourceId\":\"/subscriptions/0b62a232-b8db-4380-9da6-640f7272ed6d/resourceGroups/lfo-qa/providers/Microsoft.Web/sites/loggya/appServices\",\"key\":\""
		suffix := "\"}"
		targetSize := logs.MaxPayloadSize/2 - len(prefix) - len(suffix) - 3
		logString := fmt.Sprintf("%s%s%s", prefix, strings.Repeat("a", targetSize), suffix)
		logBytes := []byte(logString)
		for range 3 {
			currLog, err := logs.NewLog(logBytes, functionAppContainer)
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

var validLog = []byte("{ \"time\": \"2024-08-21T15:12:24Z\", \"resourceId\": \"/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':'','roleInstance':'BD28A314-638598491096328853','message':'LoggerFilterOptions\\n{\\n  \\'MinLevel\\': \\'None\\',\\n  \\'Rules\\': [\\n    {\\n      \\'ProviderName\\': null,\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'None\\',\\n      \\'Filter\\': null\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Logging.ApplicationInsights.ApplicationInsightsLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'Trace\\',\\n      \\'Filter\\': null\\n    }\\n  ]\\n}','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}")

var validBlob = storage.Blob{
	Container:     storage.Container{Name: functionAppContainer},
	Name:          "resourceId=/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING/y=2024/m=10/d=28/h=16/m=00/PT1H.json",
	ContentLength: int64(len(validLog)),
}

func TestNewLog(t *testing.T) {
	t.Parallel()

	t.Run("creates a Log from raw log", func(t *testing.T) {
		t.Parallel()
		// GIVEN

		// WHEN
		log, err := logs.NewLog(validLog, functionAppContainer)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, log.ResourceId, "/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING")
		assert.Equal(t, "FunctionAppLogs", log.Category)
		assert.Contains(t, log.Tags, "forwarder:lfo")
		assert.NotNil(t, log)

	})

	t.Run("handles an array of strings", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		var validLog = []byte("{ \"time\": \"2024-08-21T15:12:24Z\", \"resourceId\": \"/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':['app1', 'app2'],'roleInstance':'BD28A314-638598491096328853','message':'LoggerFilterOptions\\n{\\n  \\'MinLevel\\': \\'None\\',\\n  \\'Rules\\': [\\n    {\\n      \\'ProviderName\\': null,\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'None\\',\\n      \\'Filter\\': null\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Logging.ApplicationInsights.ApplicationInsightsLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'Trace\\',\\n      \\'Filter\\': null\\n    }\\n  ]\\n}','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}")

		// WHEN
		log, err := logs.NewLog(validLog, functionAppContainer)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, log.ResourceId, "/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING")
		assert.Equal(t, "FunctionAppLogs", log.Category)
		assert.Contains(t, log.Tags, "forwarder:lfo")
		assert.NotNil(t, log)

	})

	t.Run("handles an array of objects", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		var validLog = []byte("{ \"time\": \"2024-08-21T15:12:24Z\", \"resourceId\": \"/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':[{'app1': null, 'app2': true}, {'app3': 3.0}],'roleInstance':'BD28A314-638598491096328853','message':'LoggerFilterOptions\\n{\\n  \\'MinLevel\\': \\'None\\',\\n  \\'Rules\\': [\\n    {\\n      \\'ProviderName\\': null,\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'None\\',\\n      \\'Filter\\': null\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Logging.ApplicationInsights.ApplicationInsightsLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'Trace\\',\\n      \\'Filter\\': null\\n    }\\n  ]\\n}','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}")

		// WHEN
		log, err := logs.NewLog(validLog, functionAppContainer)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, log.ResourceId, "/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING")
		assert.Equal(t, "FunctionAppLogs", log.Category)
		assert.Contains(t, log.Tags, "forwarder:lfo")
		assert.NotNil(t, log)

	})

	t.Run("returns custom error on incomplete json for standard logs", func(t *testing.T) {
		t.Parallel()
		// WHEN
		log, err := logs.NewLog([]byte("{ \"time\": \"2024-08-21T15:12:24Z\", "), "something normal")

		// THEN
		assert.Error(t, err)
		assert.ErrorIs(t, err, logs.ErrIncompleteLogFile)
		assert.Nil(t, log)
	})

	t.Run("returns custom error on incomplete json for function apps", func(t *testing.T) {
		t.Parallel()
		// WHEN
		log, err := logs.NewLog([]byte("{ \"time\": \"2024-08-21T15:12:24Z\", "), functionAppContainer)

		// THEN
		assert.Error(t, err)
		assert.ErrorIs(t, err, logs.ErrUnexpectedToken)
		assert.Nil(t, log)
	})

	t.Run("returns error on invalid resource id", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		invalidResourceId := []byte("{ \"resourceId\": \"something\"}")
		// WHEN
		log, err := logs.NewLog(invalidResourceId, "something normal")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid resource ID")
		assert.Nil(t, log)
	})

	t.Run("returns error on invalid resource id for function apps", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		invalidResourceId := []byte("{ \"resourceId\": \"something\"}")
		// WHEN
		log, err := logs.NewLog(invalidResourceId, functionAppContainer)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid resource ID")
		assert.Nil(t, log)
	})
}

func TestValid(t *testing.T) {
	t.Parallel()

	t.Run("valid returns true for a valid log", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		l, err := logs.NewLog(getLogWithContent("test", 5*time.Minute), "insights-logs-functionapplogs")
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
		// Given
		content := strings.Repeat("a", logs.MaxPayloadSize)
		l, err := logs.NewLog(getLogWithContent(content, 5*time.Minute), functionAppContainer)
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
		// Given
		l, err := logs.NewLog(getLogWithContent("short content", (18*time.Hour)+time.Minute), functionAppContainer)
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
		for currLog, err := range logs.ParseLogs(closer, "insights-logs-kube-audit") {
			require.NoError(t, err)
			require.NotEqual(t, "", currLog.Category)
			require.NotEqual(t, "", currLog.ResourceId)
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
		for currLog, err := range logs.ParseLogs(closer, functionAppContainer) {
			require.NoError(t, err)
			require.NotEqual(t, "", currLog.Category)
			require.NotEqual(t, "", currLog.ResourceId)
			require.False(t, currLog.Time.IsZero())
			got += 1
		}

		// THEN
		assert.Equal(t, got, 20)
	})

}
