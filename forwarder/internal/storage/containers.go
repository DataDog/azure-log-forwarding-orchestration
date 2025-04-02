// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package storage

import (
	// stdlib
	"context"
	"errors"
	"iter"
	"slices"
	"strings"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	log "github.com/sirupsen/logrus"

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

// GetLogContainers returns an iterator over a sequence of containers which contain logs.
func (c *Client) GetLogContainers(ctx context.Context, logger *log.Entry) iter.Seq[Container] {
	containerPager := c.azBlobClient.NewListContainersPager(&azblob.ListContainersOptions{Include: azblob.ListContainersInclude{Metadata: true}})
	return collections.New(ctx, containerPager, func(item azblob.ListContainersResponse) []Container {
		return collections.FilterMap(item.ContainerItems, func(container *service.ContainerItem) (Container, bool) {
			return Container{Name: *container.Name}, !slices.Contains(IgnoredContainers, *container.Name)
		})
	}, logger)
}

// CreateContainer a container with the given name
// if container already exists, no error is returned
func (c *Client) CreateContainer(ctx context.Context, containerName string) error {
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
