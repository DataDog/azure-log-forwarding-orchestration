// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package logs

import (
	// stdlib
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	// 3p
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	log "github.com/sirupsen/logrus"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/pointer"
	customtime "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/time"
)

const (
	// MaxPayloadSize is the maximum byte size of the payload to Logs API.
	// https://docs.datadoghq.com/api/latest/logs/
	MaxPayloadSize = 4 * 1000000

	// MaxLogSize is the maximum byte size of a single log to Logs API.
	// https://docs.datadoghq.com/api/latest/logs/
	MaxLogSize = 1000000

	// MaxLogAge is the maximum age a log in the payload to Logs API.
	// https://docs.datadoghq.com/api/latest/logs/
	MaxLogAge = 18 * time.Hour

	// MaxPayloadAmount is the maximum number of logs per post to Logs API.
	// https://docs.datadoghq.com/api/latest/logs/
	MaxPayloadAmount = 950

	// DatadogStagingSite is the staging site for Datadog.
	DatadogStagingSite = "datad0g.com"
)

// ValidateDatadogLog checks if the log is valid to send to Datadog and returns the log size when it is.
func ValidateDatadogLog(log datadogV2.HTTPLogItem, now customtime.Now, logger *log.Entry) (int64, bool) {
	logBytes, err := log.MarshalJSON()
	if err != nil {
		logger.WithError(err).Warning("Failed to marshal log")
		return 0, false
	}

	var azLog *azureLog
	decoder := json.NewDecoder(bytes.NewReader([]byte(log.Message)))
	err = decoder.Decode(&azLog)
	if err != nil {
		logger.WithError(err).Warning("Failed to decode log as an azure log")
	}

	resourceId := "unknown"
	if azLog != nil {
		if r := azLog.ResourceId(); r != nil {
			resourceId = r.String()
		}
	}

	timeString, ok := log.AdditionalProperties["time"]
	if !ok {
		// log does not have a time field and cannot be validated
		logger.Warningf("Skipping log without a time field for resource %s", resourceId)
		return 0, false
	}

	parsedTime, err := time.Parse(time.RFC3339, timeString.(string))
	if err != nil {
		// log has an invalid time field and cannot be validated
		logger.WithError(err).Warningf("Skipping log with an invalid time field for resource %s", resourceId)
		return 0, false
	}

	valid := validateLog(resourceId, int64(len(logBytes)), parsedTime, now, logger)
	return int64(len(logBytes)), valid
}

// validateLog checks if a log is valid to send to Datadog given a set of constraints.
func validateLog(resourceId string, byteSize int64, logTime time.Time, now customtime.Now, logger *log.Entry) bool {
	if byteSize > MaxLogSize {
		logger.Warningf("Skipping large log at %s from %s with a size of %d", logTime.Format(time.RFC3339), resourceId, byteSize)
		return false
	}
	if logTime.Before(now().Add(-MaxLogAge)) {
		logger.Warningf("Skipping log older than 18 hours (at %s) for resource: %s", logTime.Format(time.RFC3339), resourceId)
		return false
	}
	return true
}

func newHTTPLogItem(log *Log) datadogV2.HTTPLogItem {
	additionalProperties := map[string]any{
		"time":            log.Time.Format(time.RFC3339),
		"level":           log.Level,
		"originContainer": log.Container,
		"originBlob":      log.Blob,
	}

	logItem := datadogV2.HTTPLogItem{
		Service:              pointer.Get(log.Service),
		Ddsource:             pointer.Get(log.Source),
		Ddtags:               pointer.Get(strings.Join(log.Tags, ",")),
		Message:              string(log.Content),
		AdditionalProperties: additionalProperties,
	}
	return logItem
}

// DatadogLogsSubmitter wraps around the datadogV2.LogsApi struct.
//
//go:generate mockgen -package=mocks -source=$GOFILE -destination=mocks/mock_$GOFILE
type DatadogLogsSubmitter interface {
	SubmitLog(ctx context.Context, body []datadogV2.HTTPLogItem, o ...datadogV2.SubmitLogOptionalParameters) (any, *http.Response, error)
}
