package logs_test

import (
	// stdlib
	"context"
	"io"
	"net/http"
	"testing"

	// 3p
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	// datadog
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	logmocks "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs/mocks"
)

func TestHook(t *testing.T) {
	t.Parallel()

	t.Run("logging sends a log to DD", func(t *testing.T) {
		// GIVEN
		ctrl := gomock.NewController(t)
		var submittedLogs []datadogV2.HTTPLogItem
		mockDDClient := logmocks.NewMockDatadogLogsSubmitter(ctrl)
		mockDDClient.EXPECT().SubmitLog(gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(2).DoAndReturn(func(ctx context.Context, body []datadogV2.HTTPLogItem, o ...datadogV2.SubmitLogOptionalParameters) (any, *http.Response, error) {
			submittedLogs = append(submittedLogs, body...)
			return nil, nil, nil
		})
		client := logs.NewClient(mockDDClient)

		nullLog := log.New()
		nullLog.SetOutput(io.Discard)

		nullLogger := log.NewEntry(nullLog)

		hookLog := log.New()
		hookLog.SetOutput(io.Discard)

		hookLog.AddHook(logs.NewHook(client, nullLogger))
		logger := log.NewEntry(hookLog)

		// WHEN
		logger.Info("test log")
		err := client.Flush(context.Background())
		require.NoError(t, err)

		// THEN
		assert.Equal(t, 1, len(submittedLogs))
	})
}
