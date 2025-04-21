// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package logs

import (
	// stdlib
	"context"
	"math"

	customtime "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/time"

	// 3p
	log "github.com/sirupsen/logrus"

	// datadog
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/pointer"
)

const (
	azureService = "azure"

	// maxBufferSize is the maximum buffer to use for scanning logs.
	// Logs greater than this buffer will be dropped by bufio.Scanner.
	// The buffer is defaulted to the maximum value of an integer.
	maxBufferSize = math.MaxInt32

	// initialBufferSize is the initial buffer size to use for scanning logs.
	initialBufferSize = 1024 * 1024 * 5

	// newlineBytes is the number of bytes in a newline character in utf-8.
	newlineBytes = 1

	functionAppContainer = "insights-logs-functionapplogs"
	flowEventContainer   = "insights-logs-networksecuritygroupflowevent"
)

// Client is a client for submitting logs to Datadog.
// It buffers logs and sends them in batches to the Datadog API.
// Client is not thread safe.
type Client struct {
	logsSubmitter DatadogLogsSubmitter
	logsBuffer    []datadogV2.HTTPLogItem
	currentSize   int64
	FailedLogs    []datadogV2.HTTPLogItem
}

// NewClient creates a new Client.
func NewClient(logsApi DatadogLogsSubmitter) *Client {
	return &Client{
		logsSubmitter: logsApi,
	}
}

// AddLog adds a log to the buffer for future submission.
func (c *Client) AddLog(ctx context.Context, now customtime.Now, logger *log.Entry, log *Log) (err error) {
	if !log.Validate(now, logger) {
		return nil
	}
	if c.shouldFlush(log) {
		err = c.Flush(ctx)
	}
	newLog := newHTTPLogItem(log)
	c.logsBuffer = append(c.logsBuffer, newLog)
	c.currentSize += log.ScrubbedLength()

	if err != nil {
		return err
	}
	return nil
}

// AddFormattedLog adds a datadog formatted log to the buffer for future submission.
func (c *Client) AddFormattedLog(ctx context.Context, now customtime.Now, logger *log.Entry, log datadogV2.HTTPLogItem) error {
	logBytes, valid := ValidateDatadogLog(log, now, logger)
	if !valid {
		return ErrInvalidLog
	}
	if c.shouldFlushBytes(logBytes) {
		if err := c.Flush(ctx); err != nil {
			return err
		}
	}

	c.logsBuffer = append(c.logsBuffer, log)
	c.currentSize += logBytes
	return nil
}

// Flush sends all buffered logs to the Datadog API.
func (c *Client) Flush(ctx context.Context) (err error) {
	if len(c.logsBuffer) > 0 {
		option := datadogV2.SubmitLogOptionalParameters{
			ContentEncoding: pointer.Get(datadogV2.CONTENTENCODING_GZIP),
		}
		_, _, err = c.logsSubmitter.SubmitLog(ctx, c.logsBuffer, option)

		if err != nil {
			c.FailedLogs = append(c.FailedLogs, c.logsBuffer...)
		}

		c.logsBuffer = c.logsBuffer[:0]
		c.currentSize = 0
	}

	return err
}

// shouldFlush checks if adding the current log to the buffer would result in an invalid payload.
func (c *Client) shouldFlush(log *Log) bool {
	return c.shouldFlushBytes(log.ScrubbedLength())
}

// shouldFlushBytes checks if adding a log with a given size to the buffer would result in an invalid payload.
func (c *Client) shouldFlushBytes(bytes int64) bool {
	return len(c.logsBuffer)+1 >= MaxPayloadAmount || c.currentSize+bytes >= MaxPayloadSize
}
