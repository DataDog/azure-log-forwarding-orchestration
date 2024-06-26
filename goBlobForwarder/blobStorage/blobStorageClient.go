package blobStorage

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"golang.org/x/sync/errgroup"
	"strings"
	"time"
)

//go:generate mockgen -source=$GOFILE -destination=./tests/mocks/$GOFILE -package=mocks

var _ AzureStorageClient = (*StorageClient)(nil)

type AzureStorageClient interface {
	DownloadBlobLogWithOffset(blobName string, blobContainer string, byteRange int64) ([]byte, error)
	DownloadBlobLogContent(blobName string, blobContainer string) ([]byte, error)
	GetLogsFromSpecificBlobContainer(containerName string) ([]byte, error)
	GetLogContainers() ([]string, error)
	GetLogsFromDefaultBlobContainers() ([][]byte, error)
	GoGetLogsFromChannelContainer() error
	GoGetLogContainers() error
}

type StorageClient struct {
	Context     context.Context
	InChan      chan []byte
	OutChan     chan []byte
	Group       *errgroup.Group //ErrGroupPanicHandler
	AzureClient AzureBlobClient
}

func NewStorageClient(ctx context.Context, storageAccountConnectionString string, inChan chan []byte) (*StorageClient, error) {
	client, err := azblob.NewClientFromConnectionString(storageAccountConnectionString, nil)

	//eg, ctx := NewErrGroupWithContext(ctx)
	eg, ctx := errgroup.WithContext(ctx)
	return &StorageClient{
		Context:     ctx,
		InChan:      inChan,
		OutChan:     make(chan []byte),
		Group:       eg,
		AzureClient: client,
	}, err
}

func CheckBlobIsFromCurrentHour(blobName string) bool {
	isCurrentHour := strings.Contains(blobName, fmt.Sprintf("h=%02d", time.Now().Hour()))
	if CheckBlobIsFromToday(blobName) && isCurrentHour {
		return true
	}
	return false
}

// CheckBlobIsFromToday checks if the blob is from today given the current time.Now Day and Month
// parses the blob file string to check for
// EX: resourceId=/SUBSCRIPTIONS/xxx/RESOURCEGROUPS/xxx/PROVIDERS/MICROSOFT.WEB/SITES/xxx/y=2024/m=06/d=13/h=14/m=00/PT1H.json
func CheckBlobIsFromToday(blobName string) bool {
	isCurrentYear := strings.Contains(blobName, fmt.Sprintf("y=%02d", time.Now().Year()))
	isCurrentMonth := strings.Contains(blobName, fmt.Sprintf("m=%02d", time.Now().Month()))
	isCurrentDay := strings.Contains(blobName, fmt.Sprintf("d=%02d", time.Now().Day()))
	if isCurrentYear && isCurrentMonth && isCurrentDay {
		return true
	}
	return false
}

func (c *StorageClient) DownloadBlobLogWithOffset(blobName string, blobContainer string, startByte int64) ([]byte, error) {
	// Range with an offset and zero value count indicates from the offset to the resource's end.
	cursor := azblob.HTTPRange{Offset: startByte, Count: 0}
	// Download the blob
	streamResponse, err := c.AzureClient.DownloadStream(c.Context, blobContainer, blobName, &azblob.DownloadStreamOptions{Range: cursor})
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

func (c *StorageClient) DownloadBlobLogContent(blobName string, blobContainer string) ([]byte, error) {
	// Download the blob
	get, err := c.AzureClient.DownloadStream(c.Context, blobContainer, blobName, &azblob.DownloadStreamOptions{})
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

func (c *StorageClient) GetLogsFromSpecificBlobContainer(containerName string) ([]byte, error) {
	pager := c.AzureClient.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
		Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
	})
	var blobBytes []byte
	for pager.More() {
		resp, err := pager.NextPage(c.Context)
		if err != nil {
			return blobBytes, err
		}

		for _, blob := range resp.Segment.BlobItems {
			blobLogContent, err := c.DownloadBlobLogContent(*blob.Name, containerName)
			blobBytes = append(blobBytes, blobLogContent...)
			if err != nil {
				return blobBytes, err
			}
		}
	}
	return blobBytes, nil
}

func (c *StorageClient) GetLogContainers() ([]string, error) {
	var azureLogContainerNames []string
	containerPager := c.AzureClient.NewListContainersPager(&azblob.ListContainersOptions{Include: azblob.ListContainersInclude{Metadata: true}})
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

func (c *StorageClient) GetLogsFromDefaultBlobContainers() ([][]byte, error) {
	var blobFiles [][]byte
	for _, containerName := range logContainerNames {
		pager := c.AzureClient.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
			Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
		})

		for pager.More() {
			resp, err := pager.NextPage(c.Context)
			if err != nil {
				return blobFiles, err
			}

			for _, blob := range resp.Segment.BlobItems {
				logContent, err := c.DownloadBlobLogContent(*blob.Name, containerName)
				if err != nil {
					return blobFiles, err
				}
				blobFiles = append(blobFiles, logContent)
			}
		}
	}
	return blobFiles, nil
}

func (c *StorageClient) GoGetLogsFromChannelContainer() error {
	for {
		select {
		case <-c.Context.Done():
			err := c.Group.Wait()
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("Sender GoGetLogsFromChannelContainer: Context closed")
			close(c.OutChan)
			return c.Context.Err()
		case containerName, ok := <-c.InChan:
			if !ok {
				err := c.Group.Wait()
				if err != nil {
					fmt.Println(err)
				}
				fmt.Println("Sender GoGetLogsFromChannelContainer: Channel closed")
				close(c.OutChan)
				return err
			}

			pager := c.AzureClient.NewListBlobsFlatPager(string(containerName), &azblob.ListBlobsFlatOptions{
				Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
			})
			for pager.More() {
				resp, err := pager.NextPage(c.Context)
				if err != nil {
					return err
				}
				c.Group.Go(func() error {
					for _, blob := range resp.Segment.BlobItems {
						if CheckBlobIsFromToday(*blob.Name) {
							blobBytes, err := c.DownloadBlobLogContent(*blob.Name, string(containerName))
							if blobBytes == nil {
								return err
							}
							c.OutChan <- blobBytes
						}
					}
					return err
				})
			}
		}
	}
}

func (c *StorageClient) GoGetLogContainers() error {
	containerPager := c.AzureClient.NewListContainersPager(&azblob.ListContainersOptions{Include: azblob.ListContainersInclude{Metadata: true}})
	for containerPager.More() {
		resp, err := containerPager.NextPage(c.Context)
		if err != nil {
			return err
		}
		c.Group.Go(func() error {
			if c.Context.Err() != nil {
				return c.Context.Err()
			}
			for _, container := range resp.ContainerItems {
				containerName := *container.Name
				if strings.Contains(containerName, "insights-logs-") {
					select {
					case <-c.Context.Done():
						if err != nil {
							fmt.Println(err)
						}
						fmt.Println("Sender GoGetLogContainers: Context closed")
						return c.Context.Err()
					case c.OutChan <- []byte(containerName):
						fmt.Println("Sender GoGetLogContainers: Context closed")

					}
				}
			}
			return nil
		})
	}
	err := c.Group.Wait()
	if err != nil {
		fmt.Println(err)
	}
	close(c.OutChan)
	return nil
}
