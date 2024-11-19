package logs

import (
	// stdlib
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"

	// datadog
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/environment"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)

// Log represents a log to send to Datadog.
type Log struct {
	content    []byte
	ResourceId string
	Category   string
	Tags       []string
}

// IsValid checks if the log is valid to send to Datadog.
func (l *Log) IsValid() bool {
	return len(l.content) < MaxPayloadSize
}

// Content converts the log content to a string.
func (l *Log) Content() string {
	return string(l.content)
}

// Length returns the length of the log content.
func (l *Log) Length() int {
	return len(l.content)
}

// ErrIncompleteLog is an error for when a log is incomplete.
var ErrIncompleteLog = errors.New("received a partial log")

// NewLog creates a new Log from the given log bytes.
func NewLog(blob storage.Blob, logBytes []byte) (*Log, error) {
	if logBytes[len(logBytes)-1] != '}' {
		return nil, ErrIncompleteLog
	}

	resourceId, err := blob.ResourceId()
	if err != nil {
		return nil, err
	}

	parsedId, err := arm.ParseResourceID(resourceId)
	if err != nil {
		return nil, err
	}

	return &Log{
		Category:   blob.Container.Category(),
		content:    logBytes,
		ResourceId: resourceId,
		Tags:       getTags(parsedId),
	}, nil
}

func getTags(id *arm.ResourceID) []string {
	return []string{
		"subscription_id:" + id.SubscriptionID,
		"resource_group:" + id.ResourceGroupName,
		"source:" + strings.Replace(id.ResourceType.String(), "/", ".", -1),
		"forwarder:lfo",
		"control_plane_id:" + environment.GetEnvVar(environment.CONTROL_PLANE_ID),
		"config_id:" + environment.GetEnvVar(environment.CONFIG_ID),
	}
}

// TooLargeError represents an error for when a log is too large to send to Datadog.
type TooLargeError struct {
	log Log
}

// Error returns a string representation of the TooLargeError.
func (e TooLargeError) Error() string {
	return fmt.Sprintf("large log from %s with a size of %d", e.log.ResourceId, e.log.Length())
}

// bufferSize is the maximum number of logs per post to Logs API.
// https://docs.datadoghq.com/api/latest/logs/
const bufferSize = 950

// MaxPayloadSize is the maximum byte size of the payload to Logs API.
// https://docs.datadoghq.com/api/latest/logs/
const MaxPayloadSize = 4 * 1000000

func ptr[T any](v T) *T {
	return &v
}

func newHTTPLogItem(log *Log) datadogV2.HTTPLogItem {
	logItem := datadogV2.HTTPLogItem{
		Ddsource: ptr("azure"),
		Ddtags:   ptr(strings.Join(log.Tags, ",")),
		Message:  log.Content(),
	}
	return logItem
}

// DatadogLogsSubmitter wraps around the datadogV2.LogsApi struct.
//
//go:generate mockgen -package=mocks -source=$GOFILE -destination=mocks/mock_$GOFILE
type DatadogLogsSubmitter interface {
	SubmitLog(ctx context.Context, body []datadogV2.HTTPLogItem, o ...datadogV2.SubmitLogOptionalParameters) (interface{}, *http.Response, error)
}

// Client is a client for submitting logs to Datadog.
// It buffers logs and sends them in batches to the Datadog API.
// Client is not thread safe.
type Client struct {
	logsSubmitter DatadogLogsSubmitter
	logsBuffer    []*Log
	currentSize   int
}

// NewClient creates a new Client.
func NewClient(logsApi DatadogLogsSubmitter) *Client {
	return &Client{
		logsSubmitter: logsApi,
	}
}

// AddLog adds a log to the buffer for future submission.
func (c *Client) AddLog(ctx context.Context, log *Log) (err error) {
	if !log.IsValid() {
		return TooLargeError{
			log: *log,
		}
	}
	if c.shouldFlush(log) {
		err = c.Flush(ctx)
	}
	c.logsBuffer = append(c.logsBuffer, log)
	c.currentSize += log.Length()

	if err != nil {
		return err
	}
	return nil
}

// Flush sends all buffered logs to the Datadog API.
func (c *Client) Flush(ctx context.Context) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "logs.Client.Flush")
	defer span.Finish(tracer.WithError(err))

	if len(c.logsBuffer) > 0 {
		logs := make([]datadogV2.HTTPLogItem, 0, len(c.logsBuffer))
		for _, currLog := range c.logsBuffer {
			logs = append(logs, newHTTPLogItem(currLog))
		}
		_, _, err = c.logsSubmitter.SubmitLog(ctx, logs)
		c.logsBuffer = c.logsBuffer[:0]
		c.currentSize = 0
	}

	return err
}

// shouldFlush checks if adding the current log to the buffer would result in an invalid payload.
func (c *Client) shouldFlush(log *Log) bool {
	return len(c.logsBuffer)+1 >= bufferSize || c.currentSize+log.Length() >= MaxPayloadSize
}
