package tests

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/DataDog/azure-log-forwarding-offering/goBlobForwarder/logsProcessing"
	"golang.org/x/sync/errgroup"
)

func SetupTestBinary() []byte {
	data, err := os.ReadFile("goBlobForwarder/logsProcessing/tests/test_logs.txt")
	if err != nil {
		return nil
	}

	return data
}

func SetupTestLogs() []logsProcessing.AzureLogs {
	logArray := []logsProcessing.AzureLogs{
		{
			ByteSize:      50,
			ForwarderName: "testForwarder",
			DDRequire: logsProcessing.DDLogs{
				ResourceId: "resourceId=/SUBSCRIPTIONS/xxx/RESOURCEGROUPS/xxx/PROVIDERS/MICROSOFT.WEB/SITES/xxx/",
				Category:   "testCategory",
				DDSource:   "testSource",
				Service:    "azure",
			},
			Rest: make(json.RawMessage, 0),
		},
		{
			ByteSize:      500,
			ForwarderName: "testForwarder",
			DDRequire: logsProcessing.DDLogs{
				ResourceId: "resourceId=/SUBSCRIPTIONS/nnn/RESOURCEGROUPS/nn/PROVIDERS/MICROSOFT.WEB/SITES/nn/",
				Category:   "nnCategory",
				DDSource:   "testSource",
				Service:    "azure",
			},
			Rest: make(json.RawMessage, 0),
		},
	}

	return logArray
}

func TestBlobLogFormatter_BatchBlobData(t *testing.T) {
	type fields struct {
		Context            context.Context
		Group              *errgroup.Group
		LogSplittingConfig logsProcessing.AzureLogSplittingConfig
		InChan             chan []byte
		LogsChan           chan []logsProcessing.AzureLogs
	}
	type args struct {
		data []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    [][]logsProcessing.AzureLogs
		wantErr bool
	}{
		// TODO (AZINTS-2526): Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &logsProcessing.BlobLogFormatter{
				Context:            tt.fields.Context,
				Group:              tt.fields.Group,
				LogSplittingConfig: tt.fields.LogSplittingConfig,
				InChan:             tt.fields.InChan,
				LogsChan:           tt.fields.LogsChan,
			}
			got, err := b.BatchBlobData(tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("BatchBlobData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BatchBlobData() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBlobLogFormatter_FormatBlobLogData(t *testing.T) {
	type fields struct {
		Context            context.Context
		Group              *errgroup.Group
		LogSplittingConfig logsProcessing.AzureLogSplittingConfig
		InChan             chan []byte
		LogsChan           chan []logsProcessing.AzureLogs
	}
	type args struct {
		logBytes []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    logsProcessing.AzureLogs
		want1   int
		wantErr bool
	}{
		// TODO (AZINTS-2526): Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &logsProcessing.BlobLogFormatter{
				Context:            tt.fields.Context,
				Group:              tt.fields.Group,
				LogSplittingConfig: tt.fields.LogSplittingConfig,
				InChan:             tt.fields.InChan,
				LogsChan:           tt.fields.LogsChan,
			}
			got, got1, err := b.FormatBlobLogData(tt.args.logBytes)
			if (err != nil) != tt.wantErr {
				t.Errorf("FormatBlobLogData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FormatBlobLogData() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("FormatBlobLogData() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestBlobLogFormatter_GoFormatAndBatchLogs(t *testing.T) {
	type fields struct {
		Context            context.Context
		Group              *errgroup.Group
		LogSplittingConfig logsProcessing.AzureLogSplittingConfig
		InChan             chan []byte
		LogsChan           chan []logsProcessing.AzureLogs
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO (AZINTS-2526): Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &logsProcessing.BlobLogFormatter{
				Context:            tt.fields.Context,
				Group:              tt.fields.Group,
				LogSplittingConfig: tt.fields.LogSplittingConfig,
				InChan:             tt.fields.InChan,
				LogsChan:           tt.fields.LogsChan,
			}
			if err := c.GoFormatAndBatchLogs(); (err != nil) != tt.wantErr {
				t.Errorf("GoFormatAndBatchLogs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
