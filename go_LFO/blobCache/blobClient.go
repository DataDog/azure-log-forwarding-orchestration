package blobCache

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type AzureBlobClient struct {
	client         *azblob.Client
	context        context.Context
	storageAccount string
	blobContainer  string
}

func NewBlobClient(context context.Context, storageAccount string, blobContainer string) *AzureBlobClient {
	url := fmt.Sprintf("https://%s.blob.core.windows.net/", storageAccount)

	credential, err := azidentity.NewDefaultAzureCredential(nil)
	handleError(err)

	client, err := azblob.NewClient(url, credential, nil)
	handleError(err)

	return &AzureBlobClient{
		context:        context,
		client:         client,
		storageAccount: storageAccount,
		blobContainer:  blobContainer,
	}
}

func (c *AzureBlobClient) UploadBlob(blobName string, data []byte) error {
	_, err := c.client.UploadBuffer(c.context, c.blobContainer, blobName, data, &azblob.UploadBufferOptions{})
	return err
}

func (c *AzureBlobClient) DownloadBlob(blobName string, data []byte) error {
	_, err := c.client.DownloadBuffer(c.context, c.blobContainer, blobName, data, &azblob.DownloadBufferOptions{})
	return err
}

func (c *AzureBlobClient) downloadBlobLogContent(blobName string) {
	// Download the blob
	get, err := c.client.DownloadStream(context.TODO(), c.blobContainer, blobName, nil)
	handleError(err)

	downloadedData := bytes.Buffer{}
	retryReader := get.NewRetryReader(context.TODO(), &azblob.RetryReaderOptions{})
	_, err = downloadedData.ReadFrom(retryReader)
	handleError(err)

	err = retryReader.Close()
	handleError(err)

	// Print the contents of the blob we created
	fmt.Println("Blob contents:")
	fmt.Println(downloadedData.String())
}
