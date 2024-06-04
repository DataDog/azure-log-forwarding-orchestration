package tests

import (
	"github.com/DataDog/azure-log-forwarding-offering/go_LFO/FormatAzureLogs"
	"reflect"
	"testing"
)

func TestBatch(t *testing.T) {
	type fields struct {
		MaxItemSizeBytes  int
		MaxBatchSizeBytes int
		MaxItemsCount     int
	}
	type args struct {
		items     []formatAzureLogs.AzureLogs
		totalSize int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   [][]formatAzureLogs.AzureLogs
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := formatAzureLogs.NewBatcher(tt.fields.MaxItemSizeBytes, tt.fields.MaxBatchSizeBytes, tt.fields.MaxItemsCount)
			if got := b.Batch(tt.args.items, tt.args.totalSize); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Batch() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewBatcher(t *testing.T) {
	type args struct {
		maxItemSizeBytes  int
		maxBatchSizeBytes int
		maxItemsCount     int
	}
	tests := []struct {
		name string
		args args
		want *formatAzureLogs.Batcher
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatAzureLogs.NewBatcher(tt.args.maxItemSizeBytes, tt.args.maxBatchSizeBytes, tt.args.maxItemsCount); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBatcher() = %v, want %v", got, tt.want)
			}
		})
	}
}
