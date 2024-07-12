package storage

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func (c *Client) GetContainersMatchingPrefix(ctx context.Context, parentSpan ddtrace.SpanContext, prefix string, outputChan chan []*string) error {
	currentSpan := tracer.StartSpan("storage.GetContainersMatchingPrefix", tracer.ChildOf(parentSpan))
	containerPager := c.azBlobClient.NewListContainersPager(&azblob.ListContainersOptions{Prefix: &prefix, Include: azblob.ListContainersInclude{Metadata: true}})
	for containerPager.More() {
		resp, err := containerPager.NextPage(ctx)
		if err != nil {
			currentSpan.Finish(tracer.WithError(err))
			return fmt.Errorf("error getting next page of containers: %v", err)
		}
		var containerNames []*string
		for _, container := range resp.ContainerItems {
			containerNames = append(containerNames, container.Name)
		}
		outputChan <- containerNames

	}
	currentSpan.Finish()
	return nil
}
