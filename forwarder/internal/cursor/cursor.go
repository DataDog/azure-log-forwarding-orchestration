// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package cursor

import (
	// stdlib
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	// 3p
	log "github.com/sirupsen/logrus"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)

func blobKey(containerName string, blobName string) string {
	return fmt.Sprintf("%s/%s", containerName, blobName)
}

const BlobName = "cursors.json"

type Cursors struct {
	mu     sync.RWMutex
	data   map[string]int64
	Length int
}

// Get returns the cursor for the given key or 0 if it does not exist.
func (c *Cursors) Get(containerName string, blobName string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, found := c.data[blobKey(containerName, blobName)]
	if !found {
		return 0
	}
	return value
}

// Set sets the cursor for the given key.
func (c *Cursors) Set(containerName string, blobName string, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[blobKey(containerName, blobName)] = offset
}

// Delete unsets the cursor for the given key.
func (c *Cursors) Delete(containerName string, blobName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, blobKey(containerName, blobName))
}

// JSONBytes returns the a []byte representation of the cursors.
func (c *Cursors) JSONBytes() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return json.Marshal(c.data)
}

// Save saves the cursors to storage
func (c *Cursors) Save(ctx context.Context, client *storage.Client) error {
	if ctx.Err() != nil && ctx.Err() == context.DeadlineExceeded {
		// Always save cursors - use a new context if error/timeout occurred already
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ctx = timeoutCtx
	}

	data, err := c.JSONBytes()
	if err != nil {
		return fmt.Errorf("error marshalling cursors: %w", err)
	}
	err = client.UploadBlob(ctx, storage.ForwarderContainer, BlobName, data)
	if err != nil {
		return fmt.Errorf("uploading cursors failed: %w", err)
	}
	return nil
}

// New creates a new Cursors object with the given data.
func New(data map[string]int64) *Cursors {
	if data == nil {
		data = make(map[string]int64)
	}
	return &Cursors{
		data: data,
	}
}

// Load loads the cursors from the storage client.
func Load(ctx context.Context, client *storage.Client, logger *log.Entry) (*Cursors, error) {
	data, err := client.DownloadBlob(ctx, storage.ForwarderContainer, BlobName)
	if err != nil {
		var notFoundError *storage.NotFoundError
		if errors.As(err, &notFoundError) {
			return New(nil), nil

		}
		return nil, fmt.Errorf("failed to download cursor: %w", err)
	}
	return FromBytes(data, logger), nil
}

func FromBytes(data []byte, logger *log.Entry) *Cursors {
	var cursorMap map[string]int64
	err := json.Unmarshal(data, &cursorMap)
	if err != nil {
		logger.Error(fmt.Errorf("could not unmarshal log cursors: %w", err).Error())
		return New(nil)
	}
	return New(cursorMap)
}
