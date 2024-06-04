package blobCache

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"log"
)

type AzureBlobClient struct {
	Client         *azblob.Client
	Context        context.Context
	StorageAccount string
}

func handleError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func NewBlobClient(context context.Context, storageAccount string) *AzureBlobClient {
	url := fmt.Sprintf(azureBlobURL, storageAccount)

	credential, err := azidentity.NewDefaultAzureCredential(nil)
	handleError(err)

	client, err := azblob.NewClient(url, credential, nil)
	handleError(err)

	return &AzureBlobClient{
		Context:        context,
		Client:         client,
		StorageAccount: storageAccount,
	}
}

func (c *AzureBlobClient) DownloadBlobLogWithOffset(blobName string, blobContainer string, byteRange int64) []byte {
	// Range with an offset and zero value count indicates from the offset to the resource's end.
	cursor := azblob.HTTPRange{Offset: byteRange, Count: 0}
	// Download the blob
	get, err := c.Client.DownloadStream(context.TODO(), blobContainer, blobName, &azblob.DownloadStreamOptions{Range: cursor})
	handleError(err)

	downloadedData := bytes.Buffer{}
	retryReader := get.NewRetryReader(context.TODO(), &azblob.RetryReaderOptions{})
	_, err = downloadedData.ReadFrom(retryReader)
	handleError(err)

	err = retryReader.Close()
	handleError(err)

	return downloadedData.Bytes()
}

func (c *AzureBlobClient) DownloadBlobLogContent(blobName string, blobContainer string) []byte {
	// Download the blob
	get, err := c.Client.DownloadStream(context.TODO(), blobContainer, blobName, &azblob.DownloadStreamOptions{})
	handleError(err)

	downloadedData := bytes.Buffer{}
	retryReader := get.NewRetryReader(context.TODO(), &azblob.RetryReaderOptions{})
	_, err = downloadedData.ReadFrom(retryReader)
	handleError(err)

	err = retryReader.Close()
	handleError(err)

	return downloadedData.Bytes()
	// Print the contents of the blob we created
	//fmt.Println(downloadedData.String())
}

func (c *AzureBlobClient) GetLogsFromSpecificBlobContainer(containerName string) []byte {
	pager := c.Client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
		Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
	})
	var blobByes []byte
	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		handleError(err)

		for _, blob := range resp.Segment.BlobItems {
			blobByes = append(blobByes, c.DownloadBlobLogContent(*blob.Name, containerName)...)
			fmt.Println(*blob.Name)
			//return blobByes
		}
	}
	return blobByes
}

func (c *AzureBlobClient) GetLogsFromBlobContainers() {
	for _, containerName := range logContainerNames {
		pager := c.Client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
			Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
		})

		for pager.More() {
			resp, err := pager.NextPage(context.TODO())
			handleError(err)

			for _, blob := range resp.Segment.BlobItems {
				c.DownloadBlobLogContent(*blob.Name, containerName)
				fmt.Println(*blob.Name)
			}
		}
	}
}
