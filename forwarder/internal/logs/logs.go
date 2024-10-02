package logs

import (
	"bytes"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"

	// stdlib
	"context"
	"fmt"
	"net/http"
	"strings"

	// datadog
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type Log struct {
	ByteSize   int
	Content    string
	ResourceId string
	Category   string
	Tags       []string
}

func (l *Log) IsValid() bool {
	return l.ByteSize < MaxPayloadSize
}

// NewLog creates a new Log from the given log bytes
func NewLog(logBytes []byte) (*Log, error) {
	logBytes = bytes.ReplaceAll(logBytes, []byte("'"), []byte("\""))
	log, err := unmarshall(logBytes)
	if err != nil {
		return nil, err
	}
	parsedId, err := arm.ParseResourceID(log.ResourceId)
	if err != nil {
		return nil, err
	}

	log.Tags = getResourceIdTags(parsedId)
	log.Tags = append(log.Tags, getForwarderTags()...)

	return log, nil
}

// InvalidLogError represents an error for when a log is not valid
type InvalidLogError struct {
	Message string
}

// Error returns a string representation of the InvalidLogError
func (e InvalidLogError) Error() string {
	runes := []rune(e.Message)
	var message string
	if len(runes) > 100 {
		message = string(runes[:100])
	} else {
		message = e.Message
	}
	return fmt.Sprintf("invalid log: %s", message)
}

// bufferSize is the maximum number of logs per post to Logs API
// https://docs.datadoghq.com/api/latest/logs/
const bufferSize = 1000

// maxPayloadSize is the maximum byte size of the payload to Logs API
// https://docs.datadoghq.com/api/latest/logs/
const MaxPayloadSize = 5 * 1000000

func ptr[T any](v T) *T {
	return &v
}

func newHTTPLogItem(log *Log) datadogV2.HTTPLogItem {
	logItem := datadogV2.HTTPLogItem{
		Ddsource: ptr("azure"),
		Ddtags:   ptr(strings.Join(log.Tags, ",")),
		Message:  log.Content,
	}
	return logItem
}

// DatadogLogsSubmitter wraps around the datadogV2.LogsApi struct
//
//go:generate mockgen -package=mocks -source=$GOFILE -destination=mocks/mock_$GOFILE
type DatadogLogsSubmitter interface {
	SubmitLog(ctx context.Context, body []datadogV2.HTTPLogItem, o ...datadogV2.SubmitLogOptionalParameters) (interface{}, *http.Response, error)
}

// Client is a client for submitting logs to Datadog
// It buffers logs and sends them in batches to the Datadog API
// Client is not thread safe
type Client struct {
	logsSubmitter DatadogLogsSubmitter
	logsBuffer    []*Log
	currentSize   int
}

// NewClient creates a new Client
func NewClient(logsApi DatadogLogsSubmitter) *Client {
	return &Client{
		logsSubmitter: logsApi,
	}
}

// AddLog adds a log to the buffer for future submission
func (c *Client) AddLog(ctx context.Context, log *Log) (err error) {
	if !log.IsValid() {
		return InvalidLogError{
			Message: "cannot submit log: " + log.Content,
		}
	}
	if c.shouldFlush(log) {
		err = c.Flush(ctx)
	}
	c.logsBuffer = append(c.logsBuffer, log)
	c.currentSize += log.ByteSize

	if err != nil {
		return err
	}
	return nil
}

// Flush sends all buffered logs to the Datadog API
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

// shouldFlush checks if adding the current log to the buffer would result in an invalid payload
func (c *Client) shouldFlush(log *Log) bool {
	return len(c.logsBuffer)+1 >= bufferSize || c.currentSize+log.ByteSize >= MaxPayloadSize
}
