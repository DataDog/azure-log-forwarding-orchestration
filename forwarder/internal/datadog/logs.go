package datadog

import (
	"context"
	"net/http"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
)

func NewHTTPLogItem(log logs.Log) datadogV2.HTTPLogItem {
	return datadogV2.HTTPLogItem{}
}

type LogsApiInterface interface {
	SubmitLog(ctx context.Context, body []datadogV2.HTTPLogItem, o ...datadogV2.SubmitLogOptionalParameters) (interface{}, *http.Response, error)
}

type Client struct {
	logsApi    LogsApiInterface
	logsBuffer []datadogV2.HTTPLogItem
}

func (c *Client) SubmitLog(ctx context.Context, log logs.Log) error {
	c.logsBuffer = append(c.logsBuffer, NewHTTPLogItem(log))
	if len(c.logsBuffer) >= 100 {
		return c.Flush(ctx)
	}
	return nil
}

func (c *Client) Flush(ctx context.Context) error {
	if len(c.logsBuffer) > 0 {
		_, _, err := c.logsApi.SubmitLog(ctx, c.logsBuffer)
		if err != nil {
			return err
		}
		c.logsBuffer = nil
	}
	return nil
}

func (c *Client) SubmitLogs(ctx context.Context, logs []datadogV2.HTTPLogItem) error {
	_, _, err := c.logsApi.SubmitLog(ctx, logs)
	return err
}
