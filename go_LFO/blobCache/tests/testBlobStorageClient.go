package tests

import (
	"context"
	"github.com/DataDog/azure-log-forwarding-offering/go_LFO/blobCache"
	"golang.org/x/sync/errgroup"
	"reflect"
	"testing"
)

func TestAzureStorage_DownloadBlobLogContent(t *testing.T) {
	type fields struct {
		inChan      chan []byte
		OutChan     chan []byte
		Group       *errgroup.Group
		AzureClient *blobCache.AzureClient
	}
	type args struct {
		blobName      string
		blobContainer string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobCache.AzureStorage{
				InChan:      tt.fields.inChan,
				OutChan:     tt.fields.OutChan,
				Group:       tt.fields.Group,
				AzureClient: tt.fields.AzureClient,
			}
			got, err := c.DownloadBlobLogContent(tt.args.blobName, tt.args.blobContainer)
			if (err != nil) != tt.wantErr {
				t.Errorf("DownloadBlobLogContent() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DownloadBlobLogContent() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAzureStorage_DownloadBlobLogWithOffset(t *testing.T) {
	type fields struct {
		inChan      chan []byte
		OutChan     chan []byte
		Group       *errgroup.Group
		AzureClient *blobCache.AzureClient
	}
	type args struct {
		blobName      string
		blobContainer string
		startByte     int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobCache.AzureStorage{
				InChan:      tt.fields.inChan,
				OutChan:     tt.fields.OutChan,
				Group:       tt.fields.Group,
				AzureClient: tt.fields.AzureClient,
			}
			got, err := c.DownloadBlobLogWithOffset(tt.args.blobName, tt.args.blobContainer, tt.args.startByte)
			if (err != nil) != tt.wantErr {
				t.Errorf("DownloadBlobLogWithOffset() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DownloadBlobLogWithOffset() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAzureStorage_GetLogContainers(t *testing.T) {
	type fields struct {
		inChan      chan []byte
		OutChan     chan []byte
		Group       *errgroup.Group
		AzureClient *blobCache.AzureClient
	}
	tests := []struct {
		name    string
		fields  fields
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobCache.AzureStorage{
				InChan:      tt.fields.inChan,
				OutChan:     tt.fields.OutChan,
				Group:       tt.fields.Group,
				AzureClient: tt.fields.AzureClient,
			}
			got, err := c.GetLogContainers()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetLogContainers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetLogContainers() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAzureStorage_GetLogsFromBlobContainers(t *testing.T) {
	type fields struct {
		inChan      chan []byte
		OutChan     chan []byte
		Group       *errgroup.Group
		AzureClient *blobCache.AzureClient
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
			c := &blobCache.AzureStorage{
				InChan:      tt.fields.inChan,
				OutChan:     tt.fields.OutChan,
				Group:       tt.fields.Group,
				AzureClient: tt.fields.AzureClient,
			}
			if err := c.GetLogsFromBlobContainers(); (err != nil) != tt.wantErr {
				t.Errorf("GetLogsFromBlobContainers() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAzureStorage_GetLogsFromDefaultBlobContainers(t *testing.T) {
	type fields struct {
		inChan      chan []byte
		OutChan     chan []byte
		Group       *errgroup.Group
		AzureClient *blobCache.AzureClient
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
			c := &blobCache.AzureStorage{
				InChan:      tt.fields.inChan,
				OutChan:     tt.fields.OutChan,
				Group:       tt.fields.Group,
				AzureClient: tt.fields.AzureClient,
			}
			if err := c.GetLogsFromDefaultBlobContainers(); (err != nil) != tt.wantErr {
				t.Errorf("GetLogsFromDefaultBlobContainers() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAzureStorage_GetLogsFromSpecificBlobContainer(t *testing.T) {
	type fields struct {
		inChan      chan []byte
		OutChan     chan []byte
		Group       *errgroup.Group
		AzureClient *blobCache.AzureClient
	}
	type args struct {
		containerName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobCache.AzureStorage{
				InChan:      tt.fields.inChan,
				OutChan:     tt.fields.OutChan,
				Group:       tt.fields.Group,
				AzureClient: tt.fields.AzureClient,
			}
			got, err := c.GetLogsFromSpecificBlobContainer(tt.args.containerName)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetLogsFromSpecificBlobContainer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetLogsFromSpecificBlobContainer() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAzureStorage_GoGetLogContainers(t *testing.T) {
	type fields struct {
		inChan      chan []byte
		OutChan     chan []byte
		Group       *errgroup.Group
		AzureClient *blobCache.AzureClient
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobCache.AzureStorage{
				InChan:      tt.fields.inChan,
				OutChan:     tt.fields.OutChan,
				Group:       tt.fields.Group,
				AzureClient: tt.fields.AzureClient,
			}
			c.GoGetLogContainers()
		})
	}
}

func TestAzureStorage_GoGetLogsFromChannelContainer(t *testing.T) {
	type fields struct {
		inChan      chan []byte
		OutChan     chan []byte
		Group       *errgroup.Group
		AzureClient *blobCache.AzureClient
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
			c := &blobCache.AzureStorage{
				InChan:      tt.fields.inChan,
				OutChan:     tt.fields.OutChan,
				Group:       tt.fields.Group,
				AzureClient: tt.fields.AzureClient,
			}
			if err := c.GoGetLogsFromChannelContainer(); (err != nil) != tt.wantErr {
				t.Errorf("GoGetLogsFromChannelContainer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewAzureStorageClient(t *testing.T) {
	type args struct {
		context        context.Context
		storageAccount string
		inChan         chan []byte
	}
	tests := []struct {
		name  string
		args  args
		want  error
		want1 *blobCache.AzureStorage
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := blobCache.NewAzureStorageClient(tt.args.context, tt.args.storageAccount, tt.args.inChan)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewAzureStorageClient() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("NewAzureStorageClient() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
