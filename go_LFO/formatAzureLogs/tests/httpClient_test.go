package tests

import (
	"context"
	"encoding/json"
	"github.com/DataDog/azure-log-forwarding-offering/go_LFO/FormatAzureLogs"
	"net/http"
	"reflect"
	"testing"
)

func TestHTTPClient_Send(t *testing.T) {
	type fields struct {
		context      context.Context
		functionName string
		httpOptions  *http.Request
		scrubber     *formatAzureLogs.Scrubber
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
			c := &formatAzureLogs.HTTPClient{
				Context:      tt.fields.context,
				FunctionName: tt.fields.functionName,
				HttpOptions:  tt.fields.httpOptions,
				Scrubber:     tt.fields.scrubber,
			}
			if err := c.Send(tt.args.batchedLog); (err != nil) != tt.wantErr {
				t.Errorf("Send() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHTTPClient_SendAll(t *testing.T) {
	type fields struct {
		context      context.Context
		functionName string
		httpOptions  *http.Request
		scrubber     *formatAzureLogs.Scrubber
	}
	type args struct {
		batches [][]formatAzureLogs.AzureLogs
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
			c := &formatAzureLogs.HTTPClient{
				Context:      tt.fields.context,
				FunctionName: tt.fields.functionName,
				HttpOptions:  tt.fields.httpOptions,
				Scrubber:     tt.fields.scrubber,
			}
			if err := c.SendAll(tt.args.batches); (err != nil) != tt.wantErr {
				t.Errorf("SendAll() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHTTPClient_SendWithRetry(t *testing.T) {
	type fields struct {
		context      context.Context
		functionName string
		httpOptions  *http.Request
		scrubber     *formatAzureLogs.Scrubber
	}
	type args struct {
		batch []formatAzureLogs.AzureLogs
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
			c := &formatAzureLogs.HTTPClient{
				Context:      tt.fields.context,
				FunctionName: tt.fields.functionName,
				HttpOptions:  tt.fields.httpOptions,
				Scrubber:     tt.fields.scrubber,
			}
			if err := c.SendWithRetry(tt.args.batch); (err != nil) != tt.wantErr {
				t.Errorf("SendWithRetry() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewHTTPClient(t *testing.T) {
	type args struct {
		context        context.Context
		scrubberConfig []formatAzureLogs.ScrubberRuleConfigs
	}
	tests := []struct {
		name string
		args args
		want *formatAzureLogs.HTTPClient
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatAzureLogs.NewHTTPClient(tt.args.context, tt.args.scrubberConfig); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewHTTPClient() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_marshallAppend(t *testing.T) {
	type args struct {
		azureLog formatAzureLogs.AzureLogs
	}
	tests := []struct {
		name string
		args args
		want json.RawMessage
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatAzureLogs.MarshallAppend(tt.args.azureLog); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("marshallAppend() = %v, want %v", got, tt.want)
			}
		})
	}
}
