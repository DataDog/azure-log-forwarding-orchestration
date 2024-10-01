package logs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

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
