package tests

import (
	"context"
	"reflect"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-offering/goBlobForwarder/blobStorage"
)

func TestAzureClient_DownloadBlobCursor(t *testing.T) {
	type fields struct {
		Client         *azblob.Client
		Context        context.Context
		StorageAccount string
	}
	tests := []struct {
		name   string
		fields fields
		want   error
		want1  blobStorage.CursorConfigs
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobStorage.BlobClient{
				Client:         tt.fields.Client,
				Context:        tt.fields.Context,
				StorageAccount: tt.fields.StorageAccount,
			}
			got, got1 := c.DownloadBlobCursor()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DownloadBlobCursor() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("DownloadBlobCursor() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestAzureClient_TeardownCursorCache(t *testing.T) {
	type fields struct {
		Client         *azblob.Client
		Context        context.Context
		StorageAccount string
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
			c := &blobStorage.BlobClient{
				Client:         tt.fields.Client,
				Context:        tt.fields.Context,
				StorageAccount: tt.fields.StorageAccount,
			}
			if err := c.TeardownCursorCache(); (err != nil) != tt.wantErr {
				t.Errorf("TeardownCursorCache() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAzureClient_UploadBlobCursor(t *testing.T) {
	type fields struct {
		Client         *azblob.Client
		Context        context.Context
		StorageAccount string
	}
	type args struct {
		cursorData blobStorage.CursorConfigs
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
			c := &blobStorage.BlobClient{
				Client:         tt.fields.Client,
				Context:        tt.fields.Context,
				StorageAccount: tt.fields.StorageAccount,
			}
			if err := c.UploadBlobCursor(tt.args.cursorData); (err != nil) != tt.wantErr {
				t.Errorf("UploadBlobCursor() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
