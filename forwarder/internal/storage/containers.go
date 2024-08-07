package storage

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func (c *Client) GetContainersMatchingPrefix(ctx context.Context, prefix string) Iterator[[]*service.ContainerItem, service.ListContainersResponse] {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.GetContainersMatchingPrefix")
	defer span.Finish()
	containerPager := c.azBlobClient.NewListContainersPager(&azblob.ListContainersOptions{Prefix: &prefix, Include: azblob.ListContainersInclude{Metadata: true}})
	iter := NewIterator(containerPager, func(resp service.ListContainersResponse) []*service.ContainerItem {
		return resp.ContainerItems
	}, make([]*service.ContainerItem, 0))
	return iter
}
