package FormatAzureLogs

import (
	"encoding/json"
)

type Batcher struct {
	MaxItemSizeBytes  int
	MaxBatchSizeBytes int
	MaxItemsCount     int
}

func NewBatcher(maxItemSizeBytes, maxBatchSizeBytes, maxItemsCount int) *Batcher {
	return &Batcher{
		MaxItemSizeBytes:  maxItemSizeBytes,
		MaxBatchSizeBytes: maxBatchSizeBytes,
		MaxItemsCount:     maxItemsCount,
	}
}

func (b *Batcher) Batch(items []AzureLogs, totalSize int) [][]AzureLogs {
	var batches [][]AzureLogs
	var batch []AzureLogs
	sizeBytes := 0
	sizeCount := 0
	if totalSize > b.MaxItemSizeBytes {
		return append(batches, items)
	}
	for _, item := range items {
		itemSizeBytes := item.ByteSize
		if sizeCount > 0 && (sizeCount >= b.MaxItemsCount || sizeBytes+itemSizeBytes > b.MaxBatchSizeBytes) {
			batches = append(batches, batch)
			batch = nil
			batch = append(batch, item)
			sizeBytes = 0
			sizeCount = 0
		}

		if itemSizeBytes <= b.MaxItemSizeBytes {
			batch = append(batch, item)
			sizeBytes += itemSizeBytes
			sizeCount++
		}
	}

	if sizeCount > 0 {
		batches = append(batches, batch)
	}

	return batches
}

func (b *Batcher) getSizeInBytes(v interface{}) int {
	data, _ := json.Marshal(v)
	return len(data)
}
