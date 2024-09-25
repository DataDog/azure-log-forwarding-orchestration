package storage

import (
	// stdlib
	"context"
	"errors"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	// datadog
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

// Creates a container with the given name
// if container already exists, no error is returned
func (c *Client) CreateContainer(ctx context.Context, containerName string) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.CreateContainer")
	defer span.Finish()
	_, err := c.azBlobClient.CreateContainer(ctx, containerName, nil)
	if err != nil {
		responseError := &azcore.ResponseError{}
		errors.As(err, &responseError)
		if responseError.RawResponse != nil && responseError.RawResponse.StatusCode == 409 {
			return nil
		}
		return err
	}
	return nil
}
