package tests

import (
	"context"
	"encoding/json"
	"github.com/DataDog/azure-log-forwarding-offering/goBlobForwarder/logsProcessing"
	"golang.org/x/sync/errgroup"
	"reflect"
	"testing"
)

func TestAddTagsToJsonLog(t *testing.T) {
	type args struct {
		blob *logsProcessing.AzureLogs
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logsProcessing.AddTagsToJsonLog(tt.args.blob)
		})
	}
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
		// TODO: Add test cases.
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
		// TODO: Add test cases.
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
		// TODO: Add test cases.
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

func TestBlobLogFormatter_getAzureLogFieldsFromJson(t *testing.T) {
	type fields struct {
		Context            context.Context
		Group              *errgroup.Group
		LogSplittingConfig logsProcessing.AzureLogSplittingConfig
		InChan             chan []byte
		LogsChan           chan []logsProcessing.AzureLogs
	}
	type args struct {
		logStruct *logsProcessing.AzureLogs
		tempJson  map[string]json.RawMessage
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
			b := &logsProcessing.BlobLogFormatter{
				Context:            tt.fields.Context,
				Group:              tt.fields.Group,
				LogSplittingConfig: tt.fields.LogSplittingConfig,
				InChan:             tt.fields.InChan,
				LogsChan:           tt.fields.LogsChan,
			}
			if err := b.GetAzureLogFieldsFromJson(tt.args.logStruct, tt.args.tempJson); (err != nil) != tt.wantErr {
				t.Errorf("getAzureLogFieldsFromJson() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBlobLogFormatter_unmarshallToPartialStruct(t *testing.T) {
	type fields struct {
		Context            context.Context
		Group              *errgroup.Group
		LogSplittingConfig logsProcessing.AzureLogSplittingConfig
		InChan             chan []byte
		LogsChan           chan []logsProcessing.AzureLogs
	}
	type args struct {
		azureLog []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    logsProcessing.AzureLogs
		wantErr bool
	}{
		// TODO: Add test cases.
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
			got, err := b.UnmarshallToPartialStruct(tt.args.azureLog)
			if (err != nil) != tt.wantErr {
				t.Errorf("unmarshallToPartialStruct() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("unmarshallToPartialStruct() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCreateDDTags(t *testing.T) {
	type args struct {
		tags []string
		name string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := logsProcessing.CreateDDTags(tt.args.tags, tt.args.name); got != tt.want {
				t.Errorf("CreateDDTags() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewBlobLogFormatter(t *testing.T) {
	type args struct {
		context context.Context
		inChan  chan []byte
	}
	tests := []struct {
		name string
		args args
		want logsProcessing.BlobLogFormatter
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := logsProcessing.NewBlobLogFormatter(tt.args.context, tt.args.inChan); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBlobLogFormatter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseResourceIdArray(t *testing.T) {
	type args struct {
		resourceId string
	}
	tests := []struct {
		name       string
		args       args
		wantSource string
		wantTags   []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSource, gotTags := logsProcessing.ParseResourceIdArray(tt.args.resourceId)
			if gotSource != tt.wantSource {
				t.Errorf("ParseResourceIdArray() gotSource = %v, want %v", gotSource, tt.wantSource)
			}
			if !reflect.DeepEqual(gotTags, tt.wantTags) {
				t.Errorf("ParseResourceIdArray() gotTags = %v, want %v", gotTags, tt.wantTags)
			}
		})
	}
}
