package tests

import (
	"context"
	"encoding/json"
	"github.com/DataDog/azure-log-forwarding-offering/go_LFO/FormatAzureLogs"
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
		want   []FormatAzureLogs.AzureLogs
		want1  int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &FormatAzureLogs.BlobLogFormatter{
				Context:            tt.fields.Context,
				LogSplittingConfig: tt.fields.logSplittingConfig,
			}
			got, got1 := b.ParseBlobData(tt.args.data)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseBlobData() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("ParseBlobData() got1 = %v, want %v", got1, tt.want1)
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
		want FormatAzureLogs.BlobLogFormatter
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatAzureLogs.NewBlobLogFormatter(tt.args.context); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBlobLogFormatter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_addTagsToJsonLog(t *testing.T) {
	type args struct {
		record *FormatAzureLogs.AzureLogs
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			FormatAzureLogs.AddTagsToJsonLog(tt.args.record)
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
			if got := FormatAzureLogs.CreateDDTags(tt.args.tags, tt.args.name); got != tt.want {
				t.Errorf("createDDTags() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getAzureLogFieldsFromJson(t *testing.T) {
	type args struct {
		logStruct *FormatAzureLogs.AzureLogs
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
			FormatAzureLogs.GetAzureLogFieldsFromJson(tt.args.logStruct, tt.args.tempJson)
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
			gotSource, gotTags := FormatAzureLogs.ParseResourceIdArray(tt.args.resourceId)
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
		want FormatAzureLogs.AzureLogs
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatAzureLogs.UnmarshallToPartialStruct(tt.args.azureLog); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("unmarshallToPartialStruct() = %v, want %v", got, tt.want)
			}
		})
	}
}
