package tests

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-offering/goBlobForwarder/blobStorage"
	"github.com/DataDog/azure-log-forwarding-offering/goBlobForwarder/blobStorage/tests/mocks"
	"github.com/golang/mock/gomock"
	"golang.org/x/sync/errgroup"
	"reflect"
	"testing"
	"time"
)

func TestGetLogContainers(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	tests := []struct {
		name          string
		blobName      string
		blobContainer string
		AzureClient   func() blobStorage.StorageClient
		want          []byte
		wantErr       bool
	}{
		{
			name:          "Test DownloadBlobLogContent",
			blobName:      "testBlob",
			blobContainer: "testContainer",
			AzureClient: func() blobStorage.StorageClient {
				//client, _ := azblob.NewClient(url, &azfake.TokenCredential{}, nil)
				client := mocks.NewMockAzureBlobClient(mockCtrl)
				//client := mockStorageClientClient(mockCtrl, inChan)
				client.EXPECT().NewListContainersPager(gomock.Any()).Return(nil)
				return blobStorage.StorageClient{
					Context:     context.Background(),
					AzureClient: client,
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.AzureClient()
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

func TestGetLogsFromDefaultBlobContainers(t *testing.T) {
	type fields struct {
		Context     context.Context
		InChan      chan []byte
		OutChan     chan []byte
		Group       *errgroup.Group
		AzureClient blobStorage.AzureBlobClient
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
			c := &blobStorage.StorageClient{
				Context:     tt.fields.Context,
				InChan:      tt.fields.InChan,
				OutChan:     tt.fields.OutChan,
				Group:       tt.fields.Group,
				AzureClient: tt.fields.AzureClient,
			}
			if err, _ := c.GetLogsFromDefaultBlobContainers(); (err != nil) != tt.wantErr {
				t.Errorf("GetLogsFromDefaultBlobContainers() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetLogsFromSpecificBlobContainer(t *testing.T) {
	type fields struct {
		Context     context.Context
		InChan      chan []byte
		OutChan     chan []byte
		Group       *errgroup.Group
		AzureClient blobStorage.AzureBlobClient
	}
	type args struct {
	}
	tests := []struct {
		name          string
		fields        fields
		containerName string
		want          []byte
		wantErr       bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &blobStorage.StorageClient{
				Context:     tt.fields.Context,
				InChan:      tt.fields.InChan,
				OutChan:     tt.fields.OutChan,
				Group:       tt.fields.Group,
				AzureClient: tt.fields.AzureClient,
			}
			got, err := c.GetLogsFromSpecificBlobContainer(tt.containerName)
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

func TestGoGetLogContainers(t *testing.T) {
	type fields struct {
		Context     context.Context
		InChan      chan []byte
		OutChan     chan []byte
		Group       *errgroup.Group
		AzureClient blobStorage.AzureBlobClient
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
			c := &blobStorage.StorageClient{
				Context:     tt.fields.Context,
				InChan:      tt.fields.InChan,
				OutChan:     tt.fields.OutChan,
				Group:       tt.fields.Group,
				AzureClient: tt.fields.AzureClient,
			}
			if err := c.GoGetLogContainers(); (err != nil) != tt.wantErr {
				t.Errorf("GoGetLogContainers() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGoGetLogsFromChannelContainer(t *testing.T) {
	type fields struct {
		Context     context.Context
		InChan      chan []byte
		OutChan     chan []byte
		Group       *errgroup.Group
		AzureClient blobStorage.AzureBlobClient
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
			c := &blobStorage.StorageClient{
				Context:     tt.fields.Context,
				InChan:      tt.fields.InChan,
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

func TestCheckBlobIsFrom(t *testing.T) {
	year := fmt.Sprintf("y=%02d", time.Now().Year())
	month := fmt.Sprintf("m=%02d", time.Now().Month())
	day := fmt.Sprintf("d=%02d", time.Now().Day())
	hour := fmt.Sprintf("h=%02d", time.Now().Hour())

	tests := []struct {
		name     string
		blobName string
		want     bool
	}{
		{
			name:     "Test CheckBlobIsFromToday Happy Path",
			blobName: fmt.Sprintf("PROVIDERS/MICROSOFT.WEB/SITES/xxx/%s/%s/%s/%s/m=00/PT1H.json", year, month, day, hour),
			want:     true,
		},
		{
			name:     "Test CheckBlobIsFromToday wrong Year",
			blobName: fmt.Sprintf("PROVIDERS/MICROSOFT.WEB/SITES/xxx/y=1999/%s/%s/%s/m=00/PT1H.json", month, day, hour),
			want:     false,
		},
		{
			name:     "Test CheckBlobIsFromToday wrong Month",
			blobName: fmt.Sprintf("PROVIDERS/MICROSOFT.WEB/SITES/xxx/%s/m=55/%s/%s/m=00/PT1H.json", year, day, hour),
			want:     false,
		},
		{
			name:     "Test CheckBlobIsFromToday wrong Day",
			blobName: fmt.Sprintf("PROVIDERS/MICROSOFT.WEB/SITES/xxx/%s/%s/d=33/%s/m=00/PT1H.json", year, month, hour),
			want:     false,
		},
		{
			name:     "Test CheckBlobIsFromToday wrong Hour",
			blobName: fmt.Sprintf("PROVIDERS/MICROSOFT.WEB/SITES/xxx/%s/%s/%s/h=25/m=00/PT1H.json", year, month, day),
			want:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := blobStorage.CheckBlobIsFromCurrentHour(tt.blobName); got != tt.want {
				t.Errorf("CheckBlobIsFromCurrentHour() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TODO: need to mock goroutines asnd pager before we can test any of the azure blob storage functions
// Azure does not provide a way to mock the pager or a good interface for their clients.
// A lot of the necessary code is internal/private
func mockContainerClient(mockCtrl *gomock.Controller, inChan chan []byte) blobStorage.StorageClient {

	eg := new(errgroup.Group) //mocks.NewMockErrGroup(mockCtrl)

	azurePager := mocks.NewMockAzurePager[azblob.ListContainersResponse](mockCtrl)
	azurePager.EXPECT().More().Return(true)
	azurePager.EXPECT().More().Return(false)
	azurePager.EXPECT().NextPage(gomock.Any()).Return(azblob.ListContainersResponse{}, nil)

	client := mocks.NewMockAzureBlobClient(mockCtrl)
	client.EXPECT().NewListContainersPager(gomock.Any()).Return(azurePager)

	return blobStorage.StorageClient{
		Context:     context.Background(),
		InChan:      inChan,
		OutChan:     make(chan []byte),
		AzureClient: client,
		Group:       eg, //blobStorage.ErrGroupPanicHandler{Group: eg},
	}
}
