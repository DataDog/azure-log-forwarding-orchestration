package cursor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

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
	cursors := &Cursors{}
	for key, value := range data {
		cursors.SetCursor(key, value)
	}
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

func LoadCursors(ctx context.Context, client *storage.Client, logger *log.Entry) (*Cursors, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Client.GetCursors")
	defer span.Finish()
	data, err := client.DownloadBlob(ctx, storage.ForwarderContainer, BlobName)
	if err != nil {
		var notFoundError *storage.NotFoundError
		if errors.As(err, &notFoundError) {
			return NewCursors(nil), nil

		}
		return nil, fmt.Errorf("error downloading cursor: %v", err)
	}
	var cursorMap map[string]int64
	err = json.Unmarshal(data, &cursorMap)
	if err != nil {
		logger.Errorf("could not unmarshal log cursors: %v", err)
		return NewCursors(nil), nil
	}
	return NewCursors(cursorMap), nil
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
