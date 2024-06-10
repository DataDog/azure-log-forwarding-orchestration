package tests

import (
	"context"
	"encoding/json"
	"github.com/DataDog/azure-log-forwarding-offering/go_LFO/LogsProcessing"
	"golang.org/x/sync/errgroup"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestDatadogClient_GoSendWithRetry(t *testing.T) {
	type fields struct {
		Context     context.Context
		HttpOptions *http.Request
		Scrubber    *LogsProcessing.Scrubber
		Group       *errgroup.Group
		LogsChan    chan []LogsProcessing.AzureLogs
	}
	type args struct {
		start time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &LogsProcessing.DatadogClient{
				Context:     tt.fields.Context,
				HttpOptions: tt.fields.HttpOptions,
				Scrubber:    tt.fields.Scrubber,
				Group:       tt.fields.Group,
				LogsChan:    tt.fields.LogsChan,
			}
			if err := c.GoSendWithRetry(tt.args.start); (err != nil) != tt.wantErr {
				t.Errorf("GoSendWithRetry() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDatadogClient_Send(t *testing.T) {
	type fields struct {
		Context     context.Context
		HttpOptions *http.Request
		Scrubber    *LogsProcessing.Scrubber
		Group       *errgroup.Group
		LogsChan    chan []LogsProcessing.AzureLogs
	}
	type args struct {
		batchedLog []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &LogsProcessing.DatadogClient{
				Context:     tt.fields.Context,
				HttpOptions: tt.fields.HttpOptions,
				Scrubber:    tt.fields.Scrubber,
				Group:       tt.fields.Group,
				LogsChan:    tt.fields.LogsChan,
			}
			if err := c.Send(tt.args.batchedLog); (err != nil) != tt.wantErr {
				t.Errorf("Send() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDatadogClient_SendAll(t *testing.T) {
	type fields struct {
		Context     context.Context
		HttpOptions *http.Request
		Scrubber    *LogsProcessing.Scrubber
		Group       *errgroup.Group
		LogsChan    chan []LogsProcessing.AzureLogs
	}
	type args struct {
		batches [][]LogsProcessing.AzureLogs
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &LogsProcessing.DatadogClient{
				Context:     tt.fields.Context,
				HttpOptions: tt.fields.HttpOptions,
				Scrubber:    tt.fields.Scrubber,
				Group:       tt.fields.Group,
				LogsChan:    tt.fields.LogsChan,
			}
			if err := c.SendAll(tt.args.batches); (err != nil) != tt.wantErr {
				t.Errorf("SendAll() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDatadogClient_SendWithRetry(t *testing.T) {
	type fields struct {
		Context     context.Context
		HttpOptions *http.Request
		Scrubber    *LogsProcessing.Scrubber
		Group       *errgroup.Group
		LogsChan    chan []LogsProcessing.AzureLogs
	}
	type args struct {
		batch []LogsProcessing.AzureLogs
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &LogsProcessing.DatadogClient{
				Context:     tt.fields.Context,
				HttpOptions: tt.fields.HttpOptions,
				Scrubber:    tt.fields.Scrubber,
				Group:       tt.fields.Group,
				LogsChan:    tt.fields.LogsChan,
			}
			if err := c.SendWithRetry(tt.args.batch); (err != nil) != tt.wantErr {
				t.Errorf("SendWithRetry() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMarshallAppend(t *testing.T) {
	type args struct {
		azureLog LogsProcessing.AzureLogs
	}
	tests := []struct {
		name    string
		args    args
		want    json.RawMessage
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LogsProcessing.MarshallAppend(tt.args.azureLog)
			if (err != nil) != tt.wantErr {
				t.Errorf("MarshallAppend() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MarshallAppend() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewDDClient(t *testing.T) {
	type args struct {
		context        context.Context
		logsChan       chan []LogsProcessing.AzureLogs
		scrubberConfig []LogsProcessing.ScrubberRuleConfigs
	}
	tests := []struct {
		name string
		args args
		want *LogsProcessing.DatadogClient
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LogsProcessing.NewDDClient(tt.args.context, tt.args.logsChan, tt.args.scrubberConfig); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewDDClient() = %v, want %v", got, tt.want)
			}
		})
	}
}
