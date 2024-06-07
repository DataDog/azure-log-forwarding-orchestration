package formatAzureLogs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

type DatadogClient struct {
	Context     context.Context
	HttpOptions *http.Request
	Scrubber    *Scrubber
}

func NewDDClient(context context.Context, scrubberConfig []ScrubberRuleConfigs) *DatadogClient {
	httpOptions := &http.Request{
		Method: "POST",
		URL: &url.URL{
			Scheme: "https",
			Host:   fmt.Sprintf("%s:%s", DdHttpUrl, DdHttpPort),
			Path:   "/v1/input",
		},
		Header: http.Header{
			"Content-Type": {"application/json"},
			"DD-API-KEY":   {DdApiKey},
		},
	}

	return &DatadogClient{
		Context:     context,
		HttpOptions: httpOptions,
		Scrubber:    NewScrubber(scrubberConfig)}
}

func MarshallAppend(azureLog AzureLogs) json.RawMessage {
	myRawMessage, err := json.Marshal(azureLog.DDRequire)
	if err != nil {
		panic(err)
	}
	// remove the last comma from the first marshalled struct and first comma from the second struct to join bytes
	marshalledStruct := [][]byte{myRawMessage[:len(myRawMessage)-1], azureLog.Rest[1:]}
	joinedLog := bytes.Join(marshalledStruct, []byte(","))
	return joinedLog
}

func (c *DatadogClient) SendAll(batches [][]AzureLogs) error {
	for _, batch := range batches {
		if err := c.SendWithRetry(batch); err != nil {
			return err
		}
	}
	return nil
}

func (c *DatadogClient) SendWithRetry(batch []AzureLogs) error {
	for _, azureLogs := range batch {
		marshalledLog := MarshallAppend(azureLogs)
		err := c.Send(marshalledLog)
		if err != nil {
			err = c.Send(marshalledLog)
			if err != nil {
				return fmt.Errorf("unable to send request after 2 tries, err: %v", err)
			}
		}
	}
	return nil
}

func (c *DatadogClient) Send(batchedLog []byte) error {
	batchedLog = c.Scrubber.Scrub(batchedLog)

	req, err := http.NewRequest(c.HttpOptions.Method, c.HttpOptions.URL.String(), bytes.NewBuffer(batchedLog))
	if err != nil {
		return err
	}
	req.Header = c.HttpOptions.Header

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("invalid status code %d", resp.StatusCode)
	}

	return nil
}
