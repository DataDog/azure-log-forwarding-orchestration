package storage

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func (c *Client) GetContainersMatchingPrefix(prefix string, parentSpan ddtrace.SpanContext) Iterator[[]*service.ContainerItem, service.ListContainersResponse] {
	currentSpan := tracer.StartSpan("storage.GetContainersMatchingPrefix", tracer.ChildOf(parentSpan))
	containerPager := c.azBlobClient.NewListContainersPager(&azblob.ListContainersOptions{Prefix: &prefix, Include: azblob.ListContainersInclude{Metadata: true}})
	iter := NewIterator(containerPager, func(resp service.ListContainersResponse) []*service.ContainerItem {
		return resp.ContainerItems
	}, make([]*service.ContainerItem, 0))
	currentSpan.Finish()
	return iter

}
