package blobCache

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"log"
	"strings"
)

//go:generate mockgen -source=$GOFILE -destination=./tests/mocks/$GOFILE -package=mocks

var _ AzureStorageClient = (*AzureStorage)(nil)

type AzureStorageClient interface {
	DownloadBlobLogWithOffset(blobName string, blobContainer string, byteRange int64) []byte
	DownloadBlobLogContent(blobName string, blobContainer string) []byte
	GetLogsFromSpecificBlobContainer(containerName string) []byte
	GetLogContainers() []string
	GetLogsFromBlobContainers()
	GetLogsFromDefaultBlobContainers()
}

func handleError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

type AzureStorage struct {
	inChan  chan []byte
	OutChan chan []byte
	*AzureClient
}

func NewAzureStorageClient(context context.Context, storageAccount string, inChan chan []byte) *AzureStorage {
	return &AzureStorage{
		inChan:      inChan,
		OutChan:     make(chan []byte),
		AzureClient: NewAzureBlobClient(context, storageAccount),
	}
}

func (c *AzureStorage) DownloadBlobLogWithOffset(blobName string, blobContainer string, byteRange int64) []byte {
	// Range with an offset and zero value count indicates from the offset to the resource's end.
	cursor := azblob.HTTPRange{Offset: byteRange, Count: 0}
	// Download the blob
	streamResponse, err := c.Client.DownloadStream(c.Context, blobContainer, blobName, &azblob.DownloadStreamOptions{Range: cursor})
	handleError(err)

	downloadedData := bytes.Buffer{}
	retryReader := streamResponse.NewRetryReader(c.Context, &azblob.RetryReaderOptions{})
	_, err = downloadedData.ReadFrom(retryReader)
	handleError(err)

	err = retryReader.Close()
	handleError(err)

	return downloadedData.Bytes()
}

func (c *AzureStorage) DownloadBlobLogContent(blobName string, blobContainer string) []byte {
	// Download the blob
	get, err := c.Client.DownloadStream(c.Context, blobContainer, blobName, &azblob.DownloadStreamOptions{})
	handleError(err)

	downloadedData := bytes.Buffer{}
	retryReader := get.NewRetryReader(c.Context, &azblob.RetryReaderOptions{})
	_, err = downloadedData.ReadFrom(retryReader)
	handleError(err)

	err = retryReader.Close()
	handleError(err)

	return downloadedData.Bytes()
	// Print the contents of the blob we created
	//fmt.Println(downloadedData.String())
}

func (c *AzureStorage) GetLogsFromSpecificBlobContainer(containerName string) []byte {
	pager := c.Client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
		Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
	})
	var blobByes []byte
	for pager.More() {
		resp, err := pager.NextPage(c.Context)
		handleError(err)

		for _, blob := range resp.Segment.BlobItems {
			blobByes = append(blobByes, c.DownloadBlobLogContent(*blob.Name, containerName)...)
			fmt.Println(*blob.Name)
			return blobByes
		}
	}
	return blobByes
}

func (c *AzureStorage) GetLogContainers() []string {
	var azureLogContainerNames []string
	containerPager := c.Client.NewListContainersPager(&azblob.ListContainersOptions{Include: azblob.ListContainersInclude{Metadata: true}})
	for containerPager.More() {
		resp, err := containerPager.NextPage(c.Context)
		handleError(err)
		for _, container := range resp.ContainerItems {
			containerName := *container.Name
			if strings.Contains(containerName, "insights-logs-") {
				c.OutChan <- []byte(containerName)
				azureLogContainerNames = append(azureLogContainerNames, containerName)
			}
		}
	}
	return azureLogContainerNames
}

func (c *AzureStorage) GetLogsFromBlobContainers() {
	for _, containerName := range logContainerNames {
		pager := c.Client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
			Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
		})

		for pager.More() {
			resp, err := pager.NextPage(c.Context)
			handleError(err)

			for _, blob := range resp.Segment.BlobItems {
				c.DownloadBlobLogContent(*blob.Name, containerName)
				fmt.Println(*blob.Name)
			}
		}
	}
}

func (c *AzureStorage) GetLogsFromDefaultBlobContainers() {
	for _, containerName := range logContainerNames {
		pager := c.Client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
			Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
		})

		for pager.More() {
			resp, err := pager.NextPage(c.Context)
			handleError(err)

			for _, blob := range resp.Segment.BlobItems {
				c.DownloadBlobLogContent(*blob.Name, containerName)
				fmt.Println(*blob.Name)
			}
		}
	}
}
