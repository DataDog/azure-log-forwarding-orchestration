package storage

import (
	// stdlib
	"context"
	"errors"
	"iter"
	"strings"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	log "github.com/sirupsen/logrus"

	// datadog
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/collections"
)

// Container represents a container in a Storage Account.
type Container struct {
	Name string
}

func (c *Container) Category() string {
	parts := strings.Split(c.Name, "-")
	return parts[len(parts)-1]
}

// GetContainersMatchingPrefix returns an iterator over a sequence of containers with a given prefix.
func (c *Client) GetContainersMatchingPrefix(ctx context.Context, prefix string, logger *log.Entry) iter.Seq[Container] {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.GetContainersMatchingPrefix")
	defer span.Finish()
	containerPager := c.azBlobClient.NewListContainersPager(&azblob.ListContainersOptions{Prefix: &prefix, Include: azblob.ListContainersInclude{Metadata: true}})
	return collections.New[Container, azblob.ListContainersResponse](ctx, containerPager, func(item azblob.ListContainersResponse) []Container {
		containers := make([]Container, 0, len(item.ContainerItems))
		for _, container := range item.ContainerItems {
			containers = append(containers, Container{
				Name: *container.Name,
			})
		}
		return containers
	}, logger)
}

// CreateContainer a container with the given name
// if container already exists, no error is returned
func (c *Client) CreateContainer(ctx context.Context, containerName string) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.CreateContainer")
	defer span.Finish()
	_, err := c.azBlobClient.CreateContainer(ctx, containerName, nil)
	if err != nil {
		responseError := &azcore.ResponseError{}
		errors.As(err, &responseError)
		// 409 is the status code for container already exists
		if responseError.RawResponse != nil && responseError.RawResponse.StatusCode == 409 {
			return nil
		}
		return err
	}
	return nil
}
