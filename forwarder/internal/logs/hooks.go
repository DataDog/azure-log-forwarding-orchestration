package logs

import (
	// stdlib
	"context"
	"strings"
	"time"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/pointer"

	// 3p
	"github.com/sirupsen/logrus"

	// datadog
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
)

const source = "azure-log-forwarding-orchestration"

// ServiceName is the service tag used for APM and logs about this forwarder.
const ServiceName = "dd-azure-forwarder"

// Hook is a logrus hook that sends logs to Datadog.
type Hook struct {
	client *Client
	logger *logrus.Entry
}

// NewHook creates a new Hook.
func NewHook(client *Client, logger *logrus.Entry) Hook {
	return Hook{
		client: client,
		logger: logger,
	}
}

// Levels returns the enabled log levels for the Hook.
func (h Hook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire sends the log entry to Datadog.
func (h Hook) Fire(entry *logrus.Entry) error {
	additionalProperties := map[string]string{
		"time":  entry.Time.Format(time.RFC3339),
		"level": entry.Level.String(),
	}
	log := datadogV2.HTTPLogItem{
		Message:              entry.Message,
		Ddsource:             pointer.Get(source),
		Ddtags:               pointer.Get(strings.Join(DefaultTags, ",")),
		Service:              pointer.Get(ServiceName),
		AdditionalProperties: additionalProperties,
	}
	return h.client.AddFormattedLog(context.Background(), h.logger, log)
}
