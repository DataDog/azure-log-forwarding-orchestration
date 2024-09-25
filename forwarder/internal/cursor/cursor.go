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

//type Cursors sync.Map

type Cursors struct {
	sync.Map
	Length int
}

func NewCursors(data map[string]int64) *Cursors {
	if data == nil {
		data = make(map[string]int64)
	}
	length := 0
	cursors := &Cursors{}
	for key, value := range data {
		cursors.SetCursor(key, value)
		length += 1
	}
	cursors.Length = length
	return cursors
}

func (c *Cursors) GetCursor(key string) (int64, bool) {
	value, found := c.Load(key)
	if !found {
		return 0, false
	}
	return value.(int64), true
}

func (c *Cursors) SetCursor(key string, offset int64) {
	c.Store(key, offset)
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
	cursorMap := make(map[string]int64)
	c.Range(func(key, value interface{}) bool {
		cursorMap[key.(string)] = value.(int64)
		return true
	})
	return json.Marshal(cursorMap)
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
