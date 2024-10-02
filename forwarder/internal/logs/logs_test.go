package logs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func getLogWithContent(content string) []byte {
	return []byte("{ \"time\": \"2024-08-21T15:12:24Z\", \"resourceId\": \"/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':'','roleInstance':'BD28A314-638598491096328853','message':'" + content + "','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}\n")
}

func TestAddLog(t *testing.T) {
	t.Parallel()

	t.Run("batches when over max payload size", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		var payload []logs.Log
		for range 3 {
			logBytes := make([]byte, logs.MaxPayloadSize/2-1)
			currLog := logs.Log{
				ByteSize:   len(logBytes),
				Content:    string(logBytes),
				ResourceId: "test",
				Category:   "azure",
			}
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
			errors.Join(client.AddLog(ctx, &l), err)
		}
		errors.Join(client.Flush(ctx), err)

		// THEN
		assert.NoError(t, err)
	})
}

func TestValid(t *testing.T) {
	t.Parallel()

	t.Run("valid returns true for a valid log", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		l, err := logs.NewLog(getLogWithContent("test"))
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
		l, err := logs.NewLog(getLogWithContent(content))
		require.NoError(t, err)

		// WHEN
		got := l.IsValid()

		// THEN
		assert.False(t, got)
	})
}
