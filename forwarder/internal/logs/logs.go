package logs

import (
	"context"
	"net/http"
	"strings"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
)

// BufferSize is the maximum number of logs per post to Logs API
// https://docs.datadoghq.com/api/latest/logs/
const BufferSize = 1000

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
type Client struct {
	logsSubmitter DatadogLogsSubmitter
	logsBuffer    []*Log
}

func NewClient(logsApi DatadogLogsSubmitter) *Client {
	return &Client{
		logsSubmitter: logsApi,
	}
}

func (c *Client) SubmitLog(ctx context.Context, log *Log) (err error) {
	//c.logsBuffer = append(c.logsBuffer, log)
	newBuffer := append(c.logsBuffer, log)
	c.logsBuffer = newBuffer

	if len(c.logsBuffer) >= BufferSize {
		return c.Flush(ctx)
	}
	return nil
}

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
	}

	return err
}
