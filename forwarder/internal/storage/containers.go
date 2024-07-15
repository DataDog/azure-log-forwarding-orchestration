package storage

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

func (c *Client) GetContainersMatchingPrefix(prefix string) *Iterator[*service.ContainerItem, service.ListContainersResponse] {
	containerPager := c.azBlobClient.NewListContainersPager(&azblob.ListContainersOptions{Prefix: &prefix, Include: azblob.ListContainersInclude{Metadata: true}})
	return NewIterator(containerPager, func(resp service.ListContainersResponse) []*service.ContainerItem {
		return resp.ContainerItems
	})
}
