package logs

import (
	"context"
	"errors"
	"net/http"
	"strings"

	log "github.com/sirupsen/logrus"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
)

// BufferSize is the maximum number of logs per post to Logs API
// https://docs.datadoghq.com/api/latest/logs/
const BufferSize = 1000

func ptr[T any](v T) *T {
	return &v
}

func newHTTPLogItem(log *Log) (datadogV2.HTTPLogItem, error) {
	logItem := datadogV2.HTTPLogItem{
		Ddsource: ptr("azure"),
		Ddtags:   ptr(strings.Join(log.Tags, ",")),
		Message:  log.Content,
	}
	return logItem, nil
}

// HTTPSubmitter wraps around the datadogV2.LogsApi struct
//
//go:generate mockgen -package=mocks -source=$GOFILE -destination=mocks/mock_$GOFILE
type HTTPSubmitter interface {
	SubmitLog(ctx context.Context, body []datadogV2.HTTPLogItem, o ...datadogV2.SubmitLogOptionalParameters) (interface{}, *http.Response, error)
}

type Client struct {
	submitter  HTTPSubmitter
	logsBuffer []datadogV2.HTTPLogItem
}

func NewClient(logsApi HTTPSubmitter) *Client {
	return &Client{
		submitter: logsApi,
	}
}

func (c *Client) Close(ctx context.Context) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "logs.Client.Close")
	defer span.Finish(tracer.WithError(err))
	return c.Flush(ctx)
}

func (c *Client) SubmitLog(ctx context.Context, log *Log) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "logs.Client.SubmitLog")
	defer span.Finish(tracer.WithError(err))

	logItem, err := newHTTPLogItem(log)
	if err != nil {
		return err
	}
	c.logsBuffer = append(c.logsBuffer, logItem)
	if len(c.logsBuffer) >= BufferSize {
		return c.Flush(ctx)
	}
	return nil
}

func (c *Client) Flush(ctx context.Context) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "logs.Client.Flush")
	defer span.Finish(tracer.WithError(err))

	if len(c.logsBuffer) > 0 {
		obj, resp, err := c.submitter.SubmitLog(ctx, c.logsBuffer)
		log.Printf("Response: %v", resp)
		log.Println(obj)
		if err != nil {
			return err
		}
		c.logsBuffer = nil
	}
	return nil
}

func ProcessLogs(ctx context.Context, datadogClient *Client, logsCh <-chan *Log) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "datadog.ProcessLogs")
	defer span.Finish(tracer.WithError(err))
	for logItem := range logsCh {
		currErr := datadogClient.SubmitLog(ctx, logItem)
		err = errors.Join(err, currErr)
	}
	flushErr := datadogClient.Flush(ctx)
	err = errors.Join(err, flushErr)
	return err
}
