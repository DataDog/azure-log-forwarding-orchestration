package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
)

func TestHTTPClient_Send(t *testing.T) {
	type fields struct {
		context      context.Context
		functionName string
		httpOptions  *http.Request
		scrubber     *LogsProcessing.Scrubber
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
			c := &LogsProcessing.HTTPClient{
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
		scrubber     *LogsProcessing.Scrubber
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
			c := &LogsProcessing.HTTPClient{
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
		scrubber     *LogsProcessing.Scrubber
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
			c := &LogsProcessing.HTTPClient{
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
		scrubberConfig []LogsProcessing.ScrubberRuleConfigs
	}
	tests := []struct {
		name string
		args args
		want *LogsProcessing.HTTPClient
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LogsProcessing.NewHTTPClient(tt.args.context, tt.args.scrubberConfig); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewHTTPClient() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_marshallAppend(t *testing.T) {
	type args struct {
		azureLog LogsProcessing.AzureLogs
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
			if got := LogsProcessing.MarshallAppend(tt.args.azureLog); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("marshallAppend() = %v, want %v", got, tt.want)
			}
		})
	}
}
