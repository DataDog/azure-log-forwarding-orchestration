package blobStorage

import (
	"bytes"
	"context"
	"encoding/json"

	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

//go:generate mockgen -source=$GOFILE -destination=./tests/mocks/$GOFILE -package=mocks

var _ AzureCursorClient = (*BlobCursorClient)(nil)

type AzureCursorClient interface {
	DownloadBlobCursor() (error, CursorConfigs)
	UploadBlobCursor(cursorData CursorConfigs) error
	TeardownCursorCache() error
}

type BlobCursorClient struct {
	AzureClient    AzureBlobClient
	Context        context.Context
	StorageAccount string
}

func NewAzureCursorClient(context context.Context, storageAccount string) (error, *BlobCursorClient) {
	url := fmt.Sprintf(AzureBlobURL, storageAccount)

	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return errors.New("failed to create azure credential"), nil
	}

	client, err := azblob.NewClient(url, credential, nil)
	if err != nil {
		return errors.New("failed to create azure client"), nil
	}

	return err, &BlobCursorClient{
		Context:        context,
		AzureClient:    client,
		StorageAccount: storageAccount,
	}
}

func (c *BlobCursorClient) TeardownCursorCache() error {
	_, err := c.AzureClient.DeleteBlob(c.Context, cursorContainerName, cursorBlobName, nil)
	_, err = c.AzureClient.DeleteContainer(c.Context, cursorContainerName, nil)
	return err
}

func (c *BlobCursorClient) DownloadBlobCursor() (error, CursorConfigs) {
	// Download the blob
	get, err := c.AzureClient.DownloadStream(c.Context, cursorContainerName, cursorBlobName, &azblob.DownloadStreamOptions{})
	if err != nil {
		if e, ok := err.(*azcore.ResponseError); ok && e.StatusCode == 404 {
			_, err = c.AzureClient.CreateContainer(c.Context, cursorContainerName, nil)
			if err == nil {
				err := c.UploadBlobCursor(nil)
				return err, nil
			}
		}
		return err, nil
	}

	var downloadedData bytes.Buffer
	retryReader := get.NewRetryReader(c.Context, &azblob.RetryReaderOptions{})
	_, err = downloadedData.ReadFrom(retryReader)
	retryReader.Close()

	var cursor CursorConfigs
	if err = json.Unmarshal(downloadedData.Bytes(), &cursor); err != nil {
		return err, cursor
	}

	return nil, cursor
}

func (c *BlobCursorClient) UploadBlobCursor(cursorData CursorConfigs) error {
	marshalledCursor, err := json.Marshal(cursorData)
	if err != nil {
		return err
	}
	blobContentReader := bytes.NewReader(marshalledCursor)

	// Upload the file to the specified container with the cursorBlobName
	_, err = c.AzureClient.UploadStream(c.Context, cursorContainerName, cursorBlobName, blobContentReader, nil)
	if err != nil {
		if e, ok := err.(*azcore.ResponseError); ok && e.StatusCode == 404 {
			_, err = c.AzureClient.CreateContainer(c.Context, cursorContainerName, nil)
			if err == nil {
				_, err = c.AzureClient.UploadStream(c.Context, cursorContainerName, cursorBlobName, blobContentReader, nil)
				return err
			}
		}
	}
	return err
}
