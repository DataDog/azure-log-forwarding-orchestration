// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

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

const hookSource = "azure-log-forwarding-orchestration"

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
	additionalProperties := map[string]any{
		"time":  entry.Time.Format(time.RFC3339),
		"level": entry.Level.String(),
	}
	log := datadogV2.HTTPLogItem{
		Message:              entry.Message,
		Ddsource:             pointer.Get(hookSource),
		Ddtags:               pointer.Get(strings.Join(DefaultTags, ",")),
		Service:              pointer.Get(ServiceName),
		AdditionalProperties: additionalProperties,
	}
	return h.client.AddRawLog(context.Background(), time.Now, h.logger, log)
}
