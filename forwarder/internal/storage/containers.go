package storage

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"golang.org/x/sync/errgroup"
)

func (c *Client) GetContainersMatchingPrefix(ctx context.Context, group *errgroup.Group, prefix string, outputChan chan []*string) error {
	containerPager := c.azBlobClient.NewListContainersPager(&azblob.ListContainersOptions{Prefix: &prefix, Include: azblob.ListContainersInclude{Metadata: true}})
	for containerPager.More() {
		resp, err := containerPager.NextPage(ctx)
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var containerNames []*string
		for _, container := range resp.ContainerItems {
			containerNames = append(containerNames, container.Name)
		}
		outputChan <- containerNames

	}
	return nil
}
