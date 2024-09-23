package cursor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type Cursors struct {
	cursors map[string]int64
	mu      sync.Mutex
}

func NewCursors(data map[string]int64) *Cursors {
	if data == nil {
		data = make(map[string]int64)
	}
	return &Cursors{
		cursors: data,
	}
}

func (c *Cursors) Length() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.cursors)
}

func (c *Cursors) GetCursor(key string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cursor, ok := c.cursors[key]
	if !ok {
		return 0, fmt.Errorf("cursor not found for key %s", key)
	}
	return cursor, nil
}

func (c *Cursors) SetCursor(key string, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cursors[key] = offset
}

const BlobName = "cursors.json"

func LoadCursors(ctx context.Context, client *storage.Client) (*Cursors, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.GetCursors")
	defer span.Finish()
	data, err := client.DownloadBlob(ctx, storage.ForwarderContainer, BlobName)
	if err != nil {
		if strings.Contains(err.Error(), "BlobNotFound") {
			return NewCursors(nil), nil
		}
		return nil, fmt.Errorf("error downloading cursor: %v", err)
	}
	var cursorMap map[string]int64
	err = json.Unmarshal(data, &cursorMap)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal: %v", err)
	}
	cursors := NewCursors(cursorMap)
	return cursors, nil
}

func (c *Cursors) GetRawCursors() ([]byte, error) {
	return json.Marshal(c.cursors)
}

func (c *Cursors) SaveCursors(ctx context.Context, client *storage.Client) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.SaveCursors")
	defer span.Finish()
	data, err := c.GetRawCursors()
	if err != nil {
		return fmt.Errorf("error marshalling cursors: %v", err)
	}
	return client.UploadBlob(ctx, storage.ForwarderContainer, BlobName, data)
}
