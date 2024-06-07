package blobCache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

//go:generate mockgen -source=$GOFILE -destination=./tests/mocks/$GOFILE -package=mocks

var _ AzureCursorClient = (*AzureClient)(nil)

type AzureCursorClient interface {
	DownloadBlobCursor() CursorConfigs
	UploadBlobCursor(cursorData CursorConfigs) azblob.UploadStreamResponse
	TeardownCursorCache()
}

func (c *AzureClient) TeardownCursorCache() {
	_, err := c.Client.DeleteBlob(c.Context, cursorContainerName, cursorBlobName, nil)
	handleError(err)
	_, err = c.Client.DeleteContainer(c.Context, cursorContainerName, nil)
	handleError(err)
}

func (c *AzureClient) DownloadBlobCursor() CursorConfigs {
	// Download the blob
	get, err := c.Client.DownloadStream(c.Context, cursorContainerName, cursorBlobName, &azblob.DownloadStreamOptions{})
	if err != nil {
		if e, ok := err.(*azcore.ResponseError); ok && e.StatusCode == 404 {
			_, err = c.Client.CreateContainer(c.Context, cursorContainerName, nil)
			if err == nil {
				c.UploadBlobCursor(nil)
				return nil
			}
		}
		handleError(err)
	}

	var downloadedData bytes.Buffer
	retryReader := get.NewRetryReader(c.Context, &azblob.RetryReaderOptions{})
	_, err = downloadedData.ReadFrom(retryReader)
	handleError(err)

	err = retryReader.Close()
	handleError(err)
	var cursor CursorConfigs
	if err = json.Unmarshal(downloadedData.Bytes(), &cursor); err != nil {
		panic(err)
	}

	if cursor == nil {
		fmt.Println(err)
	}

	return cursor
	// Print the contents of the blob we created
	//fmt.Println(downloadedData.String())
}

func (c *AzureClient) UploadBlobCursor(cursorData CursorConfigs) azblob.UploadStreamResponse {
	marshalledCursor, err := json.Marshal(cursorData)
	if err != nil {
		panic(err)
	}
	blobContentReader := bytes.NewReader(marshalledCursor)

	// Upload the file to the specified container with the cursorBlobName
	response, err := c.Client.UploadStream(c.Context, cursorContainerName, cursorBlobName, blobContentReader, nil)
	handleError(err)
	return response
}
