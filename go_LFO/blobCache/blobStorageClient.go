package blobCache

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"golang.org/x/sync/errgroup"
	"strings"
)

//go:generate mockgen -source=$GOFILE -destination=./tests/mocks/$GOFILE -package=mocks

var _ AzureStorageClient = (*AzureStorage)(nil)

type AzureStorageClient interface {
	DownloadBlobLogWithOffset(blobName string, blobContainer string, byteRange int64) ([]byte, error)
	DownloadBlobLogContent(blobName string, blobContainer string) ([]byte, error)
	GetLogsFromSpecificBlobContainer(containerName string) ([]byte, error)
	GetLogContainers() ([]string, error)
	GetLogsFromBlobContainers() error
	GetLogsFromDefaultBlobContainers() error
	GoGetLogsFromChannelContainer() error
	GoGetLogContainers()
}

type AzureStorage struct {
	inChan  chan []byte
	OutChan chan []byte
	Group   *errgroup.Group //TODO: move to AzureStorage not used in cursor
	*AzureClient
}

func NewAzureStorageClient(context context.Context, storageAccount string, inChan chan []byte) (error, *AzureStorage) {
	err, client := NewAzureBlobClient(context, storageAccount)
	return err, &AzureStorage{
		inChan:      inChan,
		OutChan:     make(chan []byte),
		Group:       new(errgroup.Group),
		AzureClient: client,
	}
}

func (c *AzureStorage) DownloadBlobLogWithOffset(blobName string, blobContainer string, startByte int64) ([]byte, error) {
	// Range with an offset and zero value count indicates from the offset to the resource's end.
	cursor := azblob.HTTPRange{Offset: startByte, Count: 0}
	// Download the blob
	streamResponse, err := c.Client.DownloadStream(c.Context, blobContainer, blobName, &azblob.DownloadStreamOptions{Range: cursor})
	if err != nil {
		return nil, err
	}

	downloadedData := bytes.Buffer{}
	retryReader := streamResponse.NewRetryReader(c.Context, &azblob.RetryReaderOptions{})
	_, err = downloadedData.ReadFrom(retryReader)
	if err != nil {
		return downloadedData.Bytes(), err
	}

	err = retryReader.Close()
	return downloadedData.Bytes(), err
}

func (c *AzureStorage) DownloadBlobLogContent(blobName string, blobContainer string) ([]byte, error) {
	// Download the blob
	get, err := c.Client.DownloadStream(c.Context, blobContainer, blobName, &azblob.DownloadStreamOptions{})
	if err != nil {
		return nil, err
	}

	downloadedData := bytes.Buffer{}
	retryReader := get.NewRetryReader(c.Context, &azblob.RetryReaderOptions{})
	_, err = downloadedData.ReadFrom(retryReader)
	if err != nil {
		return downloadedData.Bytes(), err
	}

	err = retryReader.Close()
	return downloadedData.Bytes(), err
}

func (c *AzureStorage) GetLogsFromSpecificBlobContainer(containerName string) ([]byte, error) {
	pager := c.Client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
		Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
	})
	var blobByes []byte
	for pager.More() {
		resp, err := pager.NextPage(c.Context)
		if err != nil {
			return blobByes, err
		}

		for _, blob := range resp.Segment.BlobItems {
			blobLogContent, err := c.DownloadBlobLogContent(*blob.Name, containerName)
			blobByes = append(blobByes, blobLogContent...)
			if err != nil {
				return blobByes, err
			}
		}
	}
	return blobByes, nil
}

func (c *AzureStorage) GetLogContainers() ([]string, error) {
	var azureLogContainerNames []string
	containerPager := c.Client.NewListContainersPager(&azblob.ListContainersOptions{Include: azblob.ListContainersInclude{Metadata: true}})
	for containerPager.More() {
		resp, err := containerPager.NextPage(c.Context)
		if err != nil {
			return azureLogContainerNames, err
		}
		for _, container := range resp.ContainerItems {
			containerName := *container.Name
			if strings.Contains(containerName, "insights-logs-") {
				c.OutChan <- []byte(containerName)
				azureLogContainerNames = append(azureLogContainerNames, containerName)
			}
		}
	}
	return azureLogContainerNames, nil
}

func (c *AzureStorage) GetLogsFromBlobContainers() error {
	for _, containerName := range logContainerNames {
		pager := c.Client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
			Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
		})

		for pager.More() {
			resp, err := pager.NextPage(c.Context)
			if err != nil {
				return err
			}

			for _, blob := range resp.Segment.BlobItems {
				c.DownloadBlobLogContent(*blob.Name, containerName)
				//fmt.Println(*blob.Name)
			}
		}
	}
	return nil
}

func (c *AzureStorage) GetLogsFromDefaultBlobContainers() error {
	for _, containerName := range logContainerNames {
		pager := c.Client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
			Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
		})

		for pager.More() {
			resp, err := pager.NextPage(c.Context)
			if err != nil {
				return err
			}

			for _, blob := range resp.Segment.BlobItems {
				c.DownloadBlobLogContent(*blob.Name, containerName)
				//fmt.Println(*blob.Name)
			}
		}
	}
	return nil
}

func (c *AzureStorage) GoGetLogsFromChannelContainer() error {
	for {
		select {
		case <-c.Context.Done():
			fmt.Println("Sender: Context closed GoGetLogsFromChannelContainer")
			c.Group.Wait()
			close(c.OutChan)
			return c.Context.Err()
		case containerName, ok := <-c.inChan:
			if !ok {
				fmt.Printf("Sender: Channel closed GoGetLogsFromChannelContainer\n")
				c.Group.Wait()
				close(c.OutChan)
				return nil
			}

			//fmt.Println(string(containerName))
			pager := c.Client.NewListBlobsFlatPager(string(containerName), &azblob.ListBlobsFlatOptions{
				Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
			})
			for pager.More() {
				resp, err := pager.NextPage(c.Context)
				c.Group.Go(func() error {
					if err != nil {
						return err
					}
					for _, blob := range resp.Segment.BlobItems {
						blobByes, err := c.DownloadBlobLogContent(*blob.Name, string(containerName))
						if blobByes == nil {
							return err
						}
						c.OutChan <- blobByes
					}
					return nil
				})
			}
		}
	}
}

func (c *AzureStorage) GoGetLogContainers() {
	var azureLogContainerNames []string
	containerPager := c.Client.NewListContainersPager(&azblob.ListContainersOptions{Include: azblob.ListContainersInclude{Metadata: true}})
	for containerPager.More() {
		resp, err := containerPager.NextPage(c.Context)
		c.Group.Go(func() error {
			if err != nil {
				return err
			}
			for _, container := range resp.ContainerItems {
				containerName := *container.Name
				if strings.Contains(containerName, "insights-logs-") {
					c.OutChan <- []byte(containerName)
					azureLogContainerNames = append(azureLogContainerNames, containerName)
				}
			}
			return nil
		})
	}
	c.Group.Wait()
	close(c.OutChan)
}
