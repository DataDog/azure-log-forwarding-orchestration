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
	Cursors map[string]int
	mu      sync.Mutex
}

func NewCursors() *Cursors {
	return &Cursors{
		Cursors: make(map[string]int),
	}
}

func (c *Cursors) GetCursor(key string) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cursor, ok := c.Cursors[key]
	if !ok {
		return 0, fmt.Errorf("cursor not found for key %s", key)
	}
	return cursor, nil
}

func (c *Cursors) SetCursor(key string, offset int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Cursors[key] = offset
}

const CursorContainer = "datadog-cursors"
const CursorBlob = "cursors.json"

func LoadCursors(ctx context.Context, client *storage.Client) (*Cursors, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.GetCursors")
	defer span.Finish()
	data, err := client.DownloadBlob(ctx, CursorContainer, CursorBlob)
	if err != nil {
		if strings.Contains(err.Error(), "The specified container does not exist") {
			return NewCursors(), nil
		}
		return nil, fmt.Errorf("error downloading cursor: %v", err)
	}
	var cursors *Cursors
	err = json.Unmarshal(data, &cursors)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal: %v", err)
	}

	return cursors, nil
}

func (c *Cursors) SaveCursors(ctx context.Context, client *storage.Client) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.SaveCursors")
	defer span.Finish()
	data, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("error marshalling cursors: %v", err)
	}
	return client.UploadBlob(ctx, CursorContainer, CursorBlob, data)
}
