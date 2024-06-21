package blobStorage

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"golang.org/x/sync/errgroup"
)

//go:generate mockgen -source=$GOFILE -destination=./tests/mocks/$GOFILE -package=mocks

var _ AzureStorageClient = (*StorageClient)(nil)

type AzureStorageClient interface {
	DownloadBlobLogWithOffset(blobName string, blobContainer string, byteRange int64) ([]byte, error)
	DownloadBlobLogContent(blobName string, blobContainer string) ([]byte, error)
	GetLogsFromSpecificBlobContainer(containerName string) ([]byte, error)
	GetLogContainers() ([]string, error)
	GetLogsFromBlobContainers() error
	GetLogsFromDefaultBlobContainers() error
	GoGetLogsFromChannelContainer() error
	GoGetLogContainers() error
}

type StorageClient struct {
	InChan  chan []byte
	OutChan chan []byte
	Group   *errgroup.Group
	*BlobClient
}

func NewStorageClient(ctx context.Context, cancel context.CancelFunc, storageAccount string, inChan chan []byte) (*StorageClient, error) {
	client, err := NewAzureBlobClient(ctx, cancel, storageAccount)
	if err != nil {
		return nil, err
	}
	eg, ctx := errgroup.WithContext(ctx)

	return &StorageClient{
		InChan:     inChan,
		OutChan:    make(chan []byte),
		Group:      eg,
		BlobClient: client,
	}, err
}

func (c *StorageClient) DownloadBlobLogWithOffset(blobName string, blobContainer string, startByte int64) ([]byte, error) {
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

func filterByDayAndHour(blobName string) bool {
	isCurrentHour := strings.Contains(blobName, fmt.Sprintf("d=%02d", time.Now().Hour()))
	if checkBlobIsFromToday(blobName) && isCurrentHour {
		return true
	}
	return false
}

// checkBlobIsFromToday checks if the blob is from today given the current time.Now Day and Month
// parses the blob file string to check for
// EX: resourceId=/SUBSCRIPTIONS/xxx/RESOURCEGROUPS/xxx/PROVIDERS/MICROSOFT.WEB/SITES/xxx/y=2024/m=06/d=13/h=14/m=00/PT1H.json
func checkBlobIsFromToday(blobName string) bool {
	isCurrentMonth := strings.Contains(blobName, fmt.Sprintf("m=%02d", time.Now().Month()))
	isCurrentDay := strings.Contains(blobName, fmt.Sprintf("d=%02d", time.Now().Day()))
	if isCurrentMonth && isCurrentDay {
		return true
	}
	return false
}

func (c *StorageClient) DownloadBlobLogContent(blobName string, blobContainer string) ([]byte, error) {
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

func (c *StorageClient) GetLogsFromSpecificBlobContainer(containerName string) ([]byte, error) {
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

func (c *StorageClient) GetLogContainers() ([]string, error) {
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

func (c *StorageClient) GetLogsFromBlobContainers() error {
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

func (c *StorageClient) GetLogsFromDefaultBlobContainers() error {
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

			pager := c.Client.NewListBlobsFlatPager(string(containerName), &azblob.ListBlobsFlatOptions{
				Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
			})
			for pager.More() {
				resp, err := pager.NextPage(c.Context)
				if err != nil {
					return err
				}
				c.Group.Go(func() error {
					for _, blob := range resp.Segment.BlobItems {
						if checkBlobIsFromToday(*blob.Name) {
							blobByes, err := c.DownloadBlobLogContent(*blob.Name, string(containerName))
							if blobByes == nil {
								return err
							}
							c.OutChan <- blobByes
						}
					}
					return err
				})
			}
		}
	}
}

func (c *StorageClient) GoGetLogContainers() error {
	containerPager := c.Client.NewListContainersPager(&azblob.ListContainersOptions{Include: azblob.ListContainersInclude{Metadata: true}})
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
