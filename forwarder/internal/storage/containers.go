package storage

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

func (c *Client) GetContainersMatchingPrefix(ctx context.Context, prefix string) ([]*string, error) {
	var containerNames = make([]*string, 0)
	containerPager := c.azBlobClient.NewListContainersPager(&azblob.ListContainersOptions{Prefix: &prefix, Include: azblob.ListContainersInclude{Metadata: true}})
	for containerPager.More() {
		resp, err := containerPager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		for _, container := range resp.ContainerItems {
			containerNames = append(containerNames, container.Name)
		}
	}
	return containerNames, nil
}
