package datadog

import (
	"context"
	"log"
	"net/http"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
)

const BUFFER_SIZE = 100

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

func (c *Client) Close() error {
	return c.Flush(context.Background())
}

func (c *Client) SubmitLog(ctx context.Context, log *logs.Log) error {
	logItem, err := NewHTTPLogItem(log)
	if err != nil {
		return err
	}
	c.logsBuffer = append(c.logsBuffer, logItem)
	if len(c.logsBuffer) >= BUFFER_SIZE {
		return c.Flush(ctx)
	}
	return nil
}

func (c *Client) Flush(ctx context.Context) error {
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

func (c *Client) SubmitLogs(ctx context.Context, logs []*logs.Log) error {
	var err error
	for _, log := range logs {
		err = c.SubmitLog(ctx, log)
		if err != nil {
			break
		}
	}
	return err
}
