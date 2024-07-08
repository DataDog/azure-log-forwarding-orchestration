package blobStorage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"log"
)

//go:generate mockgen -source=$GOFILE -destination=./tests/mocks/$GOFILE -package=mocks

var _ AzureCursorClient = (*BlobCursorClient)(nil)

type AzureCursorClient interface {
	DownloadBlobCursor(context context.Context) (error, CursorConfigs)
	UploadBlobCursor(context context.Context, cursorData CursorConfigs) error
	TeardownCursorCache(context context.Context) error
}

type BlobCursorClient struct {
	AzureClient AzureBlobClient
}

func NewBlobCursorClient(storageAccountConnectionString string) (*BlobCursorClient, error) {
	client, err := azblob.NewClientFromConnectionString(storageAccountConnectionString, nil)

	return &BlobCursorClient{
		AzureClient: client,
	}, err
}

func (c *BlobCursorClient) TeardownCursorCache(context context.Context) error {
	_, err := c.AzureClient.DeleteBlob(context, cursorContainerName, cursorBlobName, nil)
	if err != nil {
		return err
	}
	_, err = c.AzureClient.DeleteContainer(context, cursorContainerName, nil)
	return err
}

func (c *BlobCursorClient) DownloadBlobCursor(context context.Context) (error, CursorConfigs) {
	get, err := c.AzureClient.DownloadStream(context, cursorContainerName, cursorBlobName, &azblob.DownloadStreamOptions{})
	if err != nil {
		// Download the blob cursor cache but if this it the first time we are attempting to download the cursor,
		// and it does not exist, create the container and upload an empty cursor signifying a first pass
		var responseError *azcore.ResponseError
		if errors.As(err, &responseError) && responseError.StatusCode == 404 {
			_, err = c.AzureClient.CreateContainer(context, cursorContainerName, nil)
			if err == nil {
				err := c.UploadBlobCursor(context, nil)
				return err, nil
			}
		}
		return err, nil
	}

	var downloadedData bytes.Buffer
	retryReader := get.NewRetryReader(context, &azblob.RetryReaderOptions{})
	_, err = downloadedData.ReadFrom(retryReader)

	defer func() {
		err = retryReader.Close()
		if err != nil {
			log.Printf("Error closing download cursor: %v", err)
		}
	}()

	var cursor CursorConfigs
	err = json.Unmarshal(downloadedData.Bytes(), &cursor)
	return err, cursor
}

func (c *BlobCursorClient) UploadBlobCursor(context context.Context, cursorData CursorConfigs) error {
	marshalledCursor, err := json.Marshal(cursorData)
	if err != nil {
		return err
	}
	blobContentReader := bytes.NewReader(marshalledCursor)

	// Upload the file to the specified container with the cursorBlobName
	_, err = c.AzureClient.UploadStream(context, cursorContainerName, cursorBlobName, blobContentReader, nil)
	if err != nil {
		if e, ok := err.(*azcore.ResponseError); ok && e.StatusCode == 404 {
			_, err = c.AzureClient.CreateContainer(context, cursorContainerName, nil)
			if err == nil {
				_, err = c.AzureClient.UploadStream(context, cursorContainerName, cursorBlobName, blobContentReader, nil)
				return err
			}
		}
	}
	return err
}
