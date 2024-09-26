package cursor

import (
	// stdlib
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	// 3p
	log "github.com/sirupsen/logrus"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)

const BlobName = "cursors.json"

type Cursors struct {
	sync.Map
	Length int
}

// NewCursors creates a new Cursors object with the given data.
func NewCursors(data map[string]int64) *Cursors {
	if data == nil {
		data = make(map[string]int64)
	}
	cursors := &Cursors{}
	for key, value := range data {
		cursors.SetCursor(key, value)
	}
	return cursors
}

// GetCursor returns the cursor for the given key or 0 if it does not exist.
func (c *Cursors) GetCursor(key string) int64 {
	value, found := c.Load(key)
	if !found {
		return 0
	}
	return value.(int64)
}

// SetCursor sets the cursor for the given key.
func (c *Cursors) SetCursor(key string, offset int64) {
	c.Store(key, offset)
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
	var cursorMap map[string]int64
	err = json.Unmarshal(data, &cursorMap)
	if err != nil {
		logger.Errorf("could not unmarshal log cursors: %v", err)
		return NewCursors(nil), nil
	}
	return NewCursors(cursorMap), nil
}

// Bytes returns the a []byte representation of the cursors.
func (c *Cursors) Bytes() ([]byte, error) {
	cursorMap := make(map[string]int64)
	c.Range(func(key, value interface{}) bool {
		cursorMap[key.(string)] = value.(int64)
		return true
	})
	return json.Marshal(cursorMap)
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
