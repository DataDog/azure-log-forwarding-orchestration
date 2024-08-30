package storage

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/api/iterator"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func (c *Client) GetContainersMatchingPrefix(ctx context.Context, prefix string) Iterator[[]*service.ContainerItem, service.ListContainersResponse] {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.GetContainersMatchingPrefix")
	defer span.Finish()
	containerPager := c.azBlobClient.NewListContainersPager(&azblob.ListContainersOptions{Prefix: &prefix, Include: azblob.ListContainersInclude{Metadata: true}})
	iter := NewIterator(containerPager, func(resp service.ListContainersResponse) []*service.ContainerItem {
		return resp.ContainerItems
	}, make([]*service.ContainerItem, 0))
	return iter
}

func GetContainers(ctx context.Context, client *Client, containerNameCh chan<- string) error {
	// Get the containers from the storage account
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.GetContainers")
	defer span.Finish()
	defer close(containerNameCh)
	iter := client.GetContainersMatchingPrefix(ctx, LogContainerPrefix)

	for {
		containerList, err := iter.Next(ctx)

		if errors.Is(err, iterator.Done) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("getting next page of containers: %v", err)
		}

		if containerList != nil {
			for _, container := range containerList {
				if container == nil {
					continue
				}
				containerNameCh <- *container.Name
			}
		}
	}
}
