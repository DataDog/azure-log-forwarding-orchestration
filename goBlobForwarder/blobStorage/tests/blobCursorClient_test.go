package tests

import (
	"context"
	"github.com/DataDog/azure-log-forwarding-offering/goBlobForwarder/blobStorage"
	"github.com/DataDog/azure-log-forwarding-offering/goBlobForwarder/blobStorage/tests/mocks"
	"github.com/golang/mock/gomock"
	"reflect"
	"testing"
)

func TestDownloadBlobCursor(t *testing.T) {
	testStorageAccount := "testStorageAccount"
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	tests := []struct {
		name        string
		AzureClient func() blobStorage.AzureBlobClient
		Context     context.Context
		want        error
		want1       blobStorage.CursorConfigs
	}{
		{
			name: "Test DownloadBlobCursor",
			AzureClient: func() blobStorage.AzureBlobClient {
				client := mocks.NewMockAzureBlobClient(mockCtrl)
				return client
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobStorage.AzureCursor{
				AzureClient:    tt.AzureClient(),
				Context:        tt.Context,
				StorageAccount: testStorageAccount,
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

func TestUploadBlobCursor(t *testing.T) {
	type fields struct {
		AzureClient    blobStorage.AzureBlobClient
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
			c := &blobStorage.AzureCursor{
				AzureClient:    tt.fields.AzureClient,
				Context:        tt.fields.Context,
				StorageAccount: tt.fields.StorageAccount,
			}
			if err := c.UploadBlobCursor(tt.args.cursorData); (err != nil) != tt.wantErr {
				t.Errorf("UploadBlobCursor() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
