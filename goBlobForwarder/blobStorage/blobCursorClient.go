package blobStorage

import (
	"bytes"
	"encoding/json"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

//go:generate mockgen -source=$GOFILE -destination=./tests/mocks/$GOFILE -package=mocks

var _ AzureCursorClient = (*BlobClient)(nil)

type AzureCursorClient interface {
	DownloadBlobCursor() (error, CursorConfigs)
	UploadBlobCursor(cursorData CursorConfigs) error
	TeardownCursorCache() error
}

func (c *BlobClient) TeardownCursorCache() error {
	_, err := c.Client.DeleteBlob(c.Context, cursorContainerName, cursorBlobName, nil)
	_, err = c.Client.DeleteContainer(c.Context, cursorContainerName, nil)
	return err
}

func (c *BlobClient) DownloadBlobCursor() (error, CursorConfigs) {
	// Download the blob
	get, err := c.Client.DownloadStream(c.Context, cursorContainerName, cursorBlobName, &azblob.DownloadStreamOptions{})
	if err != nil {
		if e, ok := err.(*azcore.ResponseError); ok && e.StatusCode == 404 {
			_, err = c.Client.CreateContainer(c.Context, cursorContainerName, nil)
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

func (c *BlobClient) UploadBlobCursor(cursorData CursorConfigs) error {
	marshalledCursor, err := json.Marshal(cursorData)
	if err != nil {
		return err
	}
	blobContentReader := bytes.NewReader(marshalledCursor)

	// Upload the file to the specified container with the cursorBlobName
	_, err = c.Client.UploadStream(c.Context, cursorContainerName, cursorBlobName, blobContentReader, nil)
	if err != nil {
		if e, ok := err.(*azcore.ResponseError); ok && e.StatusCode == 404 {
			_, err = c.Client.CreateContainer(c.Context, cursorContainerName, nil)
			if err == nil {
				_, err = c.Client.UploadStream(c.Context, cursorContainerName, cursorBlobName, blobContentReader, nil)
				return err
			}
		}
	}
	return err
}
