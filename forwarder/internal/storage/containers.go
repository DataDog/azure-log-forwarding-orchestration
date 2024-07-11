package storage

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"golang.org/x/sync/errgroup"
)

func (c *Client) GetContainersMatchingPrefix(ctx context.Context, group *errgroup.Group, prefix string, outputChan chan []*string) error {
	containerPager := c.azBlobClient.NewListContainersPager(&azblob.ListContainersOptions{Prefix: &prefix, Include: azblob.ListContainersInclude{Metadata: true}})
	for containerPager.More() {
		resp, err := containerPager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("error getting next page of containers: %v", err)
		}
		var containerNames []*string
		for _, container := range resp.ContainerItems {
			containerNames = append(containerNames, container.Name)
		}
		outputChan <- containerNames

	}
	return nil
}
