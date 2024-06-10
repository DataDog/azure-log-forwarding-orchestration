package tests

import (
	"context"
	"github.com/DataDog/azure-log-forwarding-offering/goBlobForwarder/logsProcessing"
	"golang.org/x/sync/errgroup"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestNewDDClient(t *testing.T) {
	type args struct {
		context        context.Context
		logsChan       chan []logsProcessing.AzureLogs
		scrubberConfig []logsProcessing.ScrubberRuleConfigs
	}
	tests := []struct {
		name string
		args args
		want *logsProcessing.DatadogClient
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := logsProcessing.NewDDClient(tt.args.context, tt.args.logsChan, tt.args.scrubberConfig); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewDDClient() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDatadogClient_GoSendWithRetry(t *testing.T) {
	type fields struct {
		Context     context.Context
		HttpOptions *http.Request
		Scrubber    *logsProcessing.Scrubber
		Group       *errgroup.Group
		LogsChan    chan []logsProcessing.AzureLogs
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
			c := &logsProcessing.DatadogClient{
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
		Scrubber    *logsProcessing.Scrubber
		Group       *errgroup.Group
		LogsChan    chan []logsProcessing.AzureLogs
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
			c := &logsProcessing.DatadogClient{
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
		Scrubber    *logsProcessing.Scrubber
		Group       *errgroup.Group
		LogsChan    chan []logsProcessing.AzureLogs
	}
	type args struct {
		batches [][]logsProcessing.AzureLogs
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
			c := &logsProcessing.DatadogClient{
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
		Scrubber    *logsProcessing.Scrubber
		Group       *errgroup.Group
		LogsChan    chan []logsProcessing.AzureLogs
	}
	type args struct {
		batch []logsProcessing.AzureLogs
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
			c := &logsProcessing.DatadogClient{
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
