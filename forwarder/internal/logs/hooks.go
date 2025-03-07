package logs

import (
	"context"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/sirupsen/logrus"
)

var supportedLevels = []logrus.Level{logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel, logrus.WarnLevel, logrus.InfoLevel, logrus.DebugLevel, logrus.TraceLevel}

var source = "azure-log-forwarding-orchestration"

// ServiceName is the service tag used for APM and logs about this forwarder.
var ServiceName = "dd-azure-forwarder"

type Hook struct {
	client *Client
	logger *logrus.Entry
}

func NewHook(client *Client, logger *logrus.Entry) Hook {
	return Hook{
		client: client,
		logger: logger,
	}
}

func (h Hook) Levels() []logrus.Level {
	return supportedLevels
}

func (h Hook) Fire(entry *logrus.Entry) error {
	additionalProperties := map[string]string{
		"time":  entry.Time.Format(time.RFC3339),
		"level": entry.Level.String(),
	}
	log := datadogV2.HTTPLogItem{
		Message:              entry.Message,
		Ddsource:             &source,
		Service:              &ServiceName,
		AdditionalProperties: additionalProperties,
	}
	return h.client.AddFormattedLog(context.Background(), h.logger, log)
}
