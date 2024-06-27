package logsProcessing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/sync/errgroup"
	"log"
	"net/http"
	"net/url"
	"time"
)

type DatadogClient struct {
	Context     context.Context
	HttpOptions *http.Request
	Scrubber    *Scrubber
	Group       *errgroup.Group
	LogsChan    chan []AzureLogs
}

func NewDDClient(context context.Context, logsChan chan []AzureLogs, scrubberConfig []ScrubberRuleConfigs) *DatadogClient {
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
		Scrubber:    NewScrubber(scrubberConfig),
		Group:       new(errgroup.Group),
		LogsChan:    logsChan,
	}
}

func marshallAppend(azureLog AzureLogs) (json.RawMessage, error) {
	rawMessage, err := json.Marshal(azureLog.DDRequire)
	if err != nil {
		return nil, err
	}
	// remove the last comma from the first marshalled struct and first comma from the second struct to join bytes
	marshalledStruct := [][]byte{rawMessage[:len(rawMessage)-1], azureLog.Rest[1:]}
	joinedLog := bytes.Join(marshalledStruct, []byte(","))
	return joinedLog, nil
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
		marshalledLog, err := marshallAppend(azureLogs)
		if err != nil {
			return fmt.Errorf("unable to marshal log, err: %v", err)
		}

		err = c.Send(marshalledLog)
		if err != nil {
			err = c.Send(marshalledLog)
			if err != nil {
				return fmt.Errorf("unable to send request after 2 tries, err: %v", err)
			}
		}
	}
	return nil
}

func (c *DatadogClient) GoSendWithRetry(start time.Time) error {
	for {
		select {
		case <-c.Context.Done():
			err := c.Group.Wait()
			if err != nil {
				log.Println(err)
			}
			log.Println("Sender GoSendWithRetry: Context closed")
			return c.Context.Err()
		case batch, ok := <-c.LogsChan:
			if !ok {
				err := c.Group.Wait()
				if err != nil {
					log.Println(err)
				}
				log.Println("Sender GoSendWithRetry: Channel closed")
				return err
			}

			c.Group.Go(func() error {
				for _, azureLogs := range batch {
					marshalledLog, err := marshallAppend(azureLogs)
					if err != nil {
						return fmt.Errorf("unable to marshal log, err: %v", err)
					}

					err = c.Send(marshalledLog)
					if err != nil {
						err = c.Send(marshalledLog)
						if err != nil {
							return fmt.Errorf("unable to send request after 2 tries, err: %v", err)
						}
					}
					log.Println(time.Since(start))
				}
				return nil
			})
		}
	}
}

func (c *DatadogClient) Send(batchedLog []byte) error {
	//batchedLog = c.Scrubber.Scrub(batchedLog) TODO Scrubber is not implemented

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
