package storage

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

type ContainersIterator struct {
	pager *runtime.Pager[azblob.ListContainersResponse]
}

func (ci *ContainersIterator) Next(ctx context.Context) ([]*service.ContainerItem, bool, error) {
	if !ci.pager.More() {
		return nil, false, nil
	}

	resp, err := ci.pager.NextPage(ctx)
	if err != nil {
		return nil, false, err
	}

	return resp.ContainerItems, true, nil
}

func NewContainersIterator(pager *runtime.Pager[azblob.ListContainersResponse]) *ContainersIterator {
	return &ContainersIterator{pager: pager}
}

func (c *Client) GetContainersMatchingPrefix(prefix string) *ContainersIterator {
	containerPager := c.azBlobClient.NewListContainersPager(&azblob.ListContainersOptions{Prefix: &prefix, Include: azblob.ListContainersInclude{Metadata: true}})
	return NewContainersIterator(containerPager)
}
