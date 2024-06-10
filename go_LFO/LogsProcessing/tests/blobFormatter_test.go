package tests

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

func TestBlobLogFormatter_ParseBlobData(t *testing.T) {
	type fields struct {
		Context            context.Context
		logSplittingConfig interface{}
	}
	type args struct {
		data []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []LogsProcessing.AzureLogs
		want1  int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &LogsProcessing.BlobLogFormatter{
				Context:            tt.fields.Context,
				LogSplittingConfig: tt.fields.logSplittingConfig,
			}
			got, got1 := b.ParseBlobData(tt.args.data)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FormatAndBatchBlobData() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("FormatAndBatchBlobData() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestNewBlobLogFormatter(t *testing.T) {
	type args struct {
		context context.Context
	}
	tests := []struct {
		name string
		args args
		want LogsProcessing.BlobLogFormatter
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LogsProcessing.NewBlobLogFormatter(tt.args.context); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBlobLogFormatter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_addTagsToJsonLog(t *testing.T) {
	type args struct {
		record *LogsProcessing.AzureLogs
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			LogsProcessing.AddTagsToJsonLog(tt.args.record)
		})
	}
}

func Test_createDDTags(t *testing.T) {
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
			if got := LogsProcessing.CreateDDTags(tt.args.tags, tt.args.name); got != tt.want {
				t.Errorf("createDDTags() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getAzureLogFieldsFromJson(t *testing.T) {
	type args struct {
		logStruct *LogsProcessing.AzureLogs
		tempJson  map[string]json.RawMessage
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			LogsProcessing.GetAzureLogFieldsFromJson(tt.args.logStruct, tt.args.tempJson)
		})
	}
}

func Test_parseResourceIdArray(t *testing.T) {
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
			gotSource, gotTags := LogsProcessing.ParseResourceIdArray(tt.args.resourceId)
			if gotSource != tt.wantSource {
				t.Errorf("parseResourceIdArray() gotSource = %v, want %v", gotSource, tt.wantSource)
			}
			if !reflect.DeepEqual(gotTags, tt.wantTags) {
				t.Errorf("parseResourceIdArray() gotTags = %v, want %v", gotTags, tt.wantTags)
			}
		})
	}
}

func Test_unmarshallToPartialStruct(t *testing.T) {
	type args struct {
		azureLog []byte
	}
	tests := []struct {
		name string
		args args
		want LogsProcessing.AzureLogs
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LogsProcessing.UnmarshallToPartialStruct(tt.args.azureLog); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("unmarshallToPartialStruct() = %v, want %v", got, tt.want)
			}
		})
	}
}
