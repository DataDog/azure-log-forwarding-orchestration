package cursor

import (
	// stdlib
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

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

// GetCursor returns the cursor for the given key or 0 if it does not exist.
func (c *Cursors) GetCursor(containerName string, blobName string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, found := c.data[blobKey(containerName, blobName)]
	if !found {
		return 0
	}
	return value
}

// SetCursor sets the cursor for the given key.
func (c *Cursors) SetCursor(containerName string, blobName string, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[blobKey(containerName, blobName)] = offset
}

// Bytes returns the a []byte representation of the cursors.
func (c *Cursors) Bytes() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return json.Marshal(c.data)
}

// SaveCursors saves the cursors to storage
func (c *Cursors) SaveCursors(ctx context.Context, client *storage.Client) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.SaveCursors")
	defer span.Finish()
	data, err := c.Bytes()
	if err != nil {
		return fmt.Errorf("error marshalling cursors: %w", err)
	}
	err = client.UploadBlob(ctx, storage.ForwarderContainer, BlobName, data)
	if err != nil {
		return fmt.Errorf("uploading cursors failed: %w", err)
	}
	return nil
}

// NewCursors creates a new Cursors object with the given data.
func NewCursors(data map[string]int64) *Cursors {
	if data == nil {
		data = make(map[string]int64)
	}
	return &Cursors{
		data: data,
	}
}

// LoadCursors loads the cursors from the storage client.
func LoadCursors(ctx context.Context, client *storage.Client, logger *log.Entry) (*Cursors, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.GetCursors")
	defer span.Finish()
	data, err := client.DownloadBlob(ctx, storage.ForwarderContainer, BlobName)
	if err != nil {
		var notFoundError *storage.NotFoundError
		if errors.As(err, &notFoundError) {
			return NewCursors(nil), nil

		}
		return nil, fmt.Errorf("failed to download cursor: %w", err)
	}
	cursors, err := FromBytes(data)
	if err != nil {
		logger.Errorf(fmt.Errorf("could not unmarshal log cursors: %w", err).Error())
		return NewCursors(nil), nil
	}
	return cursors, nil
}

func FromBytes(data []byte) (*Cursors, error) {
	var cursorMap map[string]int64
	err := json.Unmarshal(data, &cursorMap)
	if err != nil {
		return nil, err
	}
	return NewCursors(cursorMap), nil
}
