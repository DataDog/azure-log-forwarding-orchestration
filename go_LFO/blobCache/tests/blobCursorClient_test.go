package tests

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-offering/go_LFO/blobCache"
	"reflect"
	"testing"
)

func TestAzureBlobClient_BlobC(t *testing.T) {
	type fields struct {
		client         *azblob.Client
		context        context.Context
		storageAccount string
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobCache.AzureBlobClient{
				Client:         tt.fields.client,
				Context:        tt.fields.context,
				StorageAccount: tt.fields.storageAccount,
			}
			c.BlobC()
		})
	}
}

func TestAzureBlobClient_DownloadBlobCursor(t *testing.T) {
	type fields struct {
		client         *azblob.Client
		context        context.Context
		storageAccount string
	}
	tests := []struct {
		name   string
		fields fields
		want   blobCache.CursorConfigs
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobCache.AzureBlobClient{
				Client:         tt.fields.client,
				Context:        tt.fields.context,
				StorageAccount: tt.fields.storageAccount,
			}
			if got := c.DownloadBlobCursor(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DownloadBlobCursor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAzureBlobClient_UploadBlobCursor(t *testing.T) {
	type fields struct {
		client         *azblob.Client
		context        context.Context
		storageAccount string
	}
	type args struct {
		cursorData blobCache.CursorConfigs
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   azblob.UploadStreamResponse
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobCache.AzureBlobClient{
				Client:         tt.fields.client,
				Context:        tt.fields.context,
				StorageAccount: tt.fields.storageAccount,
			}
			if got := c.UploadBlobCursor(tt.args.cursorData); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UploadBlobCursor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAzureBlobClient_getLogContainers(t *testing.T) {
	type fields struct {
		client         *azblob.Client
		context        context.Context
		storageAccount string
	}
	type args struct {
		defaultOnly bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobCache.AzureBlobClient{
				Client:         tt.fields.client,
				Context:        tt.fields.context,
				StorageAccount: tt.fields.storageAccount,
			}
			if got := c.GetLogContainers(tt.args.defaultOnly); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getLogContainers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAzureBlobClient_initializeCursorCacheContainer(t *testing.T) {
	type fields struct {
		client         *azblob.Client
		context        context.Context
		storageAccount string
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobCache.AzureBlobClient{
				Client:         tt.fields.client,
				Context:        tt.fields.context,
				StorageAccount: tt.fields.storageAccount,
			}
			c.InitializeCursorCacheContainer()
		})
	}
}

func TestAzureBlobClient_teardownCursorCache(t *testing.T) {
	type fields struct {
		client         *azblob.Client
		context        context.Context
		storageAccount string
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobCache.AzureBlobClient{
				Client:         tt.fields.client,
				Context:        tt.fields.context,
				StorageAccount: tt.fields.storageAccount,
			}
			c.TeardownCursorCache()
		})
	}
}
