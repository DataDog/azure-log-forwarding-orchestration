package tests

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-offering/go_LFO/blobCache"
	"reflect"
	"testing"
)

func TestAzureBlobClient_DownloadBlobLogContent(t *testing.T) {
	type fields struct {
		Client         *azblob.Client
		Context        context.Context
		StorageAccount string
	}
	type args struct {
		blobName      string
		blobContainer string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobCache.AzureBlobClient{
				Client:         tt.fields.Client,
				Context:        tt.fields.Context,
				StorageAccount: tt.fields.StorageAccount,
			}
			if got := c.DownloadBlobLogContent(tt.args.blobName, tt.args.blobContainer); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DownloadBlobLogContent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAzureBlobClient_DownloadBlobLogWithOffset(t *testing.T) {
	type fields struct {
		Client         *azblob.Client
		Context        context.Context
		StorageAccount string
	}
	type args struct {
		blobName      string
		blobContainer string
		byteRange     int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobCache.AzureBlobClient{
				Client:         tt.fields.Client,
				Context:        tt.fields.Context,
				StorageAccount: tt.fields.StorageAccount,
			}
			if got := c.DownloadBlobLogWithOffset(tt.args.blobName, tt.args.blobContainer, tt.args.byteRange); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DownloadBlobLogWithOffset() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAzureBlobClient_GetLogsFromSpecificBlobContainer(t *testing.T) {
	type fields struct {
		Client         *azblob.Client
		Context        context.Context
		StorageAccount string
	}
	type args struct {
		containerName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobCache.AzureBlobClient{
				Client:         tt.fields.Client,
				Context:        tt.fields.Context,
				StorageAccount: tt.fields.StorageAccount,
			}
			if got := c.GetLogsFromSpecificBlobContainer(tt.args.containerName); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetLogsFromSpecificBlobContainer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAzureBlobClient_getLogsFromBlobContainers(t *testing.T) {
	type fields struct {
		Client         *azblob.Client
		Context        context.Context
		StorageAccount string
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
				Client:         tt.fields.Client,
				Context:        tt.fields.Context,
				StorageAccount: tt.fields.StorageAccount,
			}
			c.GetLogsFromBlobContainers()
		})
	}
}

func TestNewBlobClient(t *testing.T) {
	type args struct {
		context        context.Context
		storageAccount string
	}
	tests := []struct {
		name string
		args args
		want *blobCache.AzureBlobClient
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := blobCache.NewBlobClient(tt.args.context, tt.args.storageAccount); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBlobClient() = %v, want %v", got, tt.want)
			}
		})
	}
}
