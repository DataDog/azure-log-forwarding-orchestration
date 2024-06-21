package tests

import (
	"reflect"
	"testing"

	"github.com/DataDog/azure-log-forwarding-offering/goBlobForwarder/blobStorage"
	"golang.org/x/sync/errgroup"
)

func TestAzureStorage_DownloadBlobLogContent(t *testing.T) {
	type fields struct {
		inChan      chan []byte
		OutChan     chan []byte
		Group       *errgroup.Group
		AzureClient *blobStorage.BlobClient
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
			c := &blobStorage.AzureStorage{
				InChan:     tt.fields.inChan,
				OutChan:    tt.fields.OutChan,
				Group:      tt.fields.Group,
				BlobClient: tt.fields.AzureClient,
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
		AzureClient *blobStorage.BlobClient
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
			c := &blobStorage.AzureStorage{
				InChan:     tt.fields.inChan,
				OutChan:    tt.fields.OutChan,
				Group:      tt.fields.Group,
				BlobClient: tt.fields.AzureClient,
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
		AzureClient *blobStorage.BlobClient
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
			c := &blobStorage.AzureStorage{
				InChan:     tt.fields.inChan,
				OutChan:    tt.fields.OutChan,
				Group:      tt.fields.Group,
				BlobClient: tt.fields.AzureClient,
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
		AzureClient *blobStorage.BlobClient
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
			c := &blobStorage.AzureStorage{
				InChan:     tt.fields.inChan,
				OutChan:    tt.fields.OutChan,
				Group:      tt.fields.Group,
				BlobClient: tt.fields.AzureClient,
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
		AzureClient *blobStorage.BlobClient
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
			c := &blobStorage.AzureStorage{
				InChan:     tt.fields.inChan,
				OutChan:    tt.fields.OutChan,
				Group:      tt.fields.Group,
				BlobClient: tt.fields.AzureClient,
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
		AzureClient *blobStorage.BlobClient
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
			c := &blobStorage.AzureStorage{
				InChan:     tt.fields.inChan,
				OutChan:    tt.fields.OutChan,
				Group:      tt.fields.Group,
				BlobClient: tt.fields.AzureClient,
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
		AzureClient *blobStorage.BlobClient
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobStorage.AzureStorage{
				InChan:     tt.fields.inChan,
				OutChan:    tt.fields.OutChan,
				Group:      tt.fields.Group,
				BlobClient: tt.fields.AzureClient,
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
		AzureClient *blobStorage.BlobClient
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
			c := &blobStorage.AzureStorage{
				InChan:     tt.fields.inChan,
				OutChan:    tt.fields.OutChan,
				Group:      tt.fields.Group,
				BlobClient: tt.fields.AzureClient,
			}
			if err := c.GoGetLogsFromChannelContainer(); (err != nil) != tt.wantErr {
				t.Errorf("GoGetLogsFromChannelContainer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
