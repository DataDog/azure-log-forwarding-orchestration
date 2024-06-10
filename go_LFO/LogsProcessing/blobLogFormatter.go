package LogsProcessing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/sync/errgroup"
	"strings"
)

type BlobLogFormatter struct {
	Context            context.Context
	Group              *errgroup.Group
	LogSplittingConfig AzureLogSplittingConfig
	InChan             chan []byte
	LogsChan           chan []AzureLogs
}

func NewBlobLogFormatter(context context.Context, inChan chan []byte) BlobLogFormatter {
	return BlobLogFormatter{
		Context:            context,
		Group:              new(errgroup.Group),
		LogSplittingConfig: getLogSplittingConfig(),
		LogsChan:           make(chan []AzureLogs),
		InChan:             inChan,
	}
}

func (b *BlobLogFormatter) GetAzureLogFieldsFromJson(logStruct *AzureLogs, tempJson map[string]json.RawMessage) error {
	if err := json.Unmarshal(tempJson["resourceId"], &logStruct.DDRequire.ResourceId); err != nil {
		return err
	}
	delete(tempJson, "resourceId")

	if err := json.Unmarshal(tempJson["category"], &logStruct.DDRequire.Category); err != nil {
		return err
	}
	delete(tempJson, "category")
	return nil
}

func (b *BlobLogFormatter) UnmarshallToPartialStruct(azureLog []byte) (AzureLogs, error) {
	var err error
	// partially unmarshall json to struct and keep remaining data as Raw json
	var azureStruct AzureLogs
	tempJson := make(map[string]json.RawMessage)
	if err = json.Unmarshal(azureLog, &tempJson); err != nil {
		return azureStruct, err
	}

	err = b.GetAzureLogFieldsFromJson(&azureStruct, tempJson)
	azureStruct.Rest, _ = json.Marshal(tempJson)
	return azureStruct, err
}

func (b *BlobLogFormatter) FormatBlobLogData(logBytes []byte) (AzureLogs, int, error) {
	logBytes = bytes.ReplaceAll(logBytes, []byte("'"), []byte("\""))

	azureStruct, err := b.UnmarshallToPartialStruct(logBytes)
	if err != nil {
		return azureStruct, 0, err
	}

	AddTagsToJsonLog(&azureStruct)
	byteSize := len(logBytes)
	azureStruct.ByteSize = byteSize

	return azureStruct, byteSize, nil
}

func (b *BlobLogFormatter) BatchBlobData(data []byte) ([][]AzureLogs, error) {
	var batches [][]AzureLogs
	var batch []AzureLogs
	sizeBytes := 0
	sizeCount := 0
	logs := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	for _, azureLog := range logs {
		formattedLog, itemSizeBytes, err := b.FormatBlobLogData(azureLog)
		if err != nil {
			return batches, err // should we instead log the error and continue?
		}

		if sizeCount > 0 && (sizeCount >= MaxItemsCount || sizeBytes+itemSizeBytes > MaxBatchSizeBytes) {
			batches = append(batches, batch)
			b.LogsChan <- batch
			batch = nil
			batch = append(batch, formattedLog)
			sizeBytes = itemSizeBytes
			sizeCount = 1
		}

		if itemSizeBytes <= MaxItemSizeBytes {
			batch = append(batch, formattedLog)
			sizeBytes += itemSizeBytes
			sizeCount++
		}
	}

	if sizeCount > 0 {
		b.LogsChan <- batch
		batches = append(batches, batch)
	}
	return batches, nil
}

func (c *BlobLogFormatter) GoFormatAndBatchLogs() error {
	for {
		select {
		case <-c.Context.Done():
			fmt.Printf("Sender %s: Channel closed\n", "GoFormatAndBatchLogs")
			c.Group.Wait()
			close(c.LogsChan)
			return c.Context.Err()
		case blobLog, ok := <-c.InChan:
			if !ok {
				fmt.Printf("Sender %s: Channel closed\n", "GoFormatAndBatchLogs")
				c.Group.Wait()
				close(c.LogsChan)
				return nil
			}
			c.Group.Go(func() error {
				_, err := c.BatchBlobData(blobLog)
				return err
			})
		}
	}
}

func CreateDDTags(tags []string, name string) string {
	forwarderVersionTag := "forwarderversion:" + VERSION
	tags = append(tags, forwarderVersionTag)

	if name != "" {
		forwarderNameTag := "forwardername:" + name
		tags = append(tags, forwarderNameTag)
	}
	if DdTags != "" {
		tags = append(tags, DdTags)
	}

	ddTags := strings.Join(tags, ",")
	return ddTags
}

func AddTagsToJsonLog(blob *AzureLogs) {
	source, tags := ParseResourceIdArray(blob.DDRequire.ResourceId)
	blob.DDRequire.Ddsource = source
	blob.DDRequire.Ddtags = CreateDDTags(tags, blob.ForwarderName)

	blob.DDRequire.Ddsourcecategory = DdSourceCategory
	blob.DDRequire.Service = DdService
}

func ParseResourceIdArray(resourceId string) (source string, tags []string) {
	// Convert a valid resource ID to an array, handling beginning/ending slashes
	resourceIdArray := strings.Split(strings.ToLower(strings.TrimSpace(resourceId)), "/")
	for i := range resourceIdArray {
		switch resourceIdArray[i] {
		case "subscriptions":
			i += 1
			tags = append(tags, "subscription_id:"+resourceIdArray[i])
		case "providers":
			i += 1
			// looks for the source inside the resourceID PROVIDERS/MICROSOFT.WEB and wakes everything after .
			source = "azure." + strings.Split(resourceIdArray[i], ".")[1]
		case "resourcegroups":
			i += 1
			tags = append(tags, "resource_group:"+resourceIdArray[i])
		}
	}
	if source == "" {
		source = DdSource
	}
	return source, tags
}
