package storage

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

func GetContainersMatchingPrefix(ctx context.Context, client AzureBlobClient, prefix string) ([]*string, error) {
	var containerNames = make([]*string, 0)
	containerPager := client.NewListContainersPager(&azblob.ListContainersOptions{Prefix: &prefix, Include: azblob.ListContainersInclude{Metadata: true}})
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
