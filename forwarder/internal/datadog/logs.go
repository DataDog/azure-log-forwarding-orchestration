package datadog

import (
	"context"
	"log"
	"net/http"
	"strings"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
)

const BufferSize = 100

func NewHTTPLogItem(log *logs.Log) (datadogV2.HTTPLogItem, error) {
	message, err := log.Json.MarshalJSON()
	if err != nil {
		return datadogV2.HTTPLogItem{}, err
	}

	logItem := datadogV2.HTTPLogItem{
		Ddsource: to.Ptr("azure"),
		Ddtags:   to.Ptr(strings.Join(log.Tags, ",")),
		Message:  string(message),
	}
	return logItem, nil
}

// LogsApiInterface wraps around the datadogV2.LogsApi struct
//
//go:generate mockgen -package=mocks -source=$GOFILE -destination=mocks/mock_$GOFILE
type LogsApiInterface interface {
	SubmitLog(ctx context.Context, body []datadogV2.HTTPLogItem, o ...datadogV2.SubmitLogOptionalParameters) (interface{}, *http.Response, error)
}

type Client struct {
	logsApi    LogsApiInterface
	logsBuffer []datadogV2.HTTPLogItem
}

func NewClient(logsApi LogsApiInterface) *Client {
	return &Client{
		logsApi: logsApi,
	}
}

func (c *Client) Close(ctx context.Context) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "logs.Client.SubmitLog")
	defer span.Finish(tracer.WithError(err))
	return c.Flush(ctx)
}

func (c *Client) SubmitLog(ctx context.Context, log *logs.Log) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "logs.Client.SubmitLog")
	defer span.Finish(tracer.WithError(err))

	logItem, err := NewHTTPLogItem(log)
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
		obj, resp, err := c.logsApi.SubmitLog(ctx, c.logsBuffer)
		log.Printf("Response: %v", resp)
		log.Println(obj)
		if err != nil {
			return err
		}
		c.logsBuffer = nil
	}
	return nil
}

func (c *Client) SubmitLogs(ctx context.Context, logs []*logs.Log) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "logs.Client.SubmitLog")
	defer span.Finish(tracer.WithError(err))
	for _, log := range logs {
		err = c.SubmitLog(ctx, log)
		if err != nil {
			break
		}
	}
	return err
}
