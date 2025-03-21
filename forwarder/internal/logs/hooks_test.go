package logs_test

import (
	// stdlib
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	// 3p
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	// datadog
	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
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
		mockDDClient := logmocks.NewMockDatadogApiClient(ctrl)
		mockDDClient.EXPECT().GetConfig().Return(datadog.NewConfiguration()).AnyTimes()
		mockDDClient.EXPECT().PrepareRequest(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(prepareRequest)
		mockDDClient.EXPECT().Decode(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
		mockDDClient.EXPECT().CallAPI(gomock.Any()).AnyTimes().DoAndReturn(func(req *http.Request) (*http.Response, error) {
			var currLogs []datadogV2.HTTPLogItem
			err := json.NewDecoder(req.Body).Decode(&currLogs)
			if err != nil {
				return nil, fmt.Errorf("failed to decode logs: %w", err)
			}
			submittedLogs = append(submittedLogs, currLogs...)
			currResp := &http.Response{}
			currResp.Body = io.NopCloser(strings.NewReader(""))
			return currResp, nil
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
