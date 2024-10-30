package logs_test

import (
	// stdlib
	"context"
	"errors"
	"testing"

	// 3p
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs/mocks"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)

func getLogWithContent(content string) []byte {
	return []byte("{ \"time\": \"2024-08-21T15:12:24Z\", \"resourceId\": \"/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':'','roleInstance':'BD28A314-638598491096328853','message':'" + content + "','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}")
}

func TestAddLog(t *testing.T) {
	t.Parallel()

	t.Run("batches when over max payload size", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		var payload []*logs.Log
		for range 3 {
			logBytes := make([]byte, logs.MaxPayloadSize/2-1)
			logBytes[0] = '{'
			logBytes[len(logBytes)-1] = '}'
			currLog, err := logs.NewLog(storage.Blob{
				Container: storage.Container{Name: "insights-logs-functionapplogs"},
				Name:      "resourceId=/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING/y=2024/m=10/d=28/h=16/m=00/PT1H.json",
			}, logBytes)
			require.NoError(t, err)
			payload = append(payload, currLog)
		}

		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockDatadogLogsSubmitter(ctrl)
		mockClient.EXPECT().SubmitLog(gomock.Any(), gomock.Any(), gomock.Any()).Times(2)

		client := logs.NewClient(mockClient)
		ctx := context.Background()
		var err error

		// WHEN
		for _, l := range payload {
			errors.Join(client.AddLog(ctx, l), err)
		}
		errors.Join(client.Flush(ctx), err)

		// THEN
		assert.NoError(t, err)
	})
}

var validLog = []byte("{ \"time\": \"2024-08-21T15:12:24Z\", \"resourceId\": \"/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':'','roleInstance':'BD28A314-638598491096328853','message':'LoggerFilterOptions\\n{\\n  \\'MinLevel\\': \\'None\\',\\n  \\'Rules\\': [\\n    {\\n      \\'ProviderName\\': null,\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'None\\',\\n      \\'Filter\\': null\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Logging.ApplicationInsights.ApplicationInsightsLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'Trace\\',\\n      \\'Filter\\': null\\n    }\\n  ]\\n}','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}")

var validBlob = storage.Blob{
	Container:     storage.Container{Name: "insights-logs-functionapplogs"},
	Name:          "resourceId=/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING/y=2024/m=10/d=28/h=16/m=00/PT1H.json",
	ContentLength: int64(len(validLog)),
}

func TestNewLog(t *testing.T) {
	t.Parallel()

	t.Run("creates a Log from raw log", func(t *testing.T) {
		t.Parallel()
		// GIVEN

		// WHEN
		log, err := logs.NewLog(validBlob, validLog)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, log.ResourceId, "/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING")
		assert.Equal(t, log.Category, "functionapplogs")
		assert.Contains(t, log.Tags, "forwarder:lfo")
		assert.NotNil(t, log)

	})

	t.Run("returns custom error on incomplete json", func(t *testing.T) {
		t.Parallel()
		// WHEN
		log, err := logs.NewLog(validBlob, []byte("{ \"time\": \"2024-08-21T15:12:24Z\", "))

		// THEN
		assert.Error(t, err)
		assert.ErrorIs(t, err, logs.ErrIncompleteLog)
		assert.Nil(t, log)
	})

	t.Run("returns error on invalid resource id", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		var invalidBlob = storage.Blob{
			Container:     storage.Container{Name: "insights-logs-functionapplogs"},
			Name:          "resourceId=/something/y=2024/m=10/d=28/h=16/m=00/PT1H.json",
			ContentLength: int64(len(validLog)),
		}
		invalidResourceId := []byte("{ \"resourceId\": \"something\"}")
		// WHEN
		log, err := logs.NewLog(invalidBlob, invalidResourceId)

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
		l, err := logs.NewLog(validBlob, getLogWithContent("test"))
		require.NoError(t, err)

		// WHEN
		got := l.IsValid()

		// THEN
		assert.True(t, got)
	})

	t.Run("valid returns false for an invalid log", func(t *testing.T) {
		t.Parallel()
		// Given
		oneHundredAs := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		var content string
		for range logs.MaxPayloadSize / 100 {
			content += oneHundredAs
		}
		l, err := logs.NewLog(validBlob, getLogWithContent(content))
		require.NoError(t, err)

		// WHEN
		got := l.IsValid()

		// THEN
		assert.False(t, got)
	})
}
