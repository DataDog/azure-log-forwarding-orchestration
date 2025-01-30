package deadletterqueue

import (
	// stdlib
	"context"
	"encoding/json"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	// datadog
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)

// BlobName is the name of the blob that contains the dead letter queue.
const BlobName = "deadletterqueue.json"

// DeadLetterQueue is a queue of logs that failed to be sent to Datadog.
type DeadLetterQueue struct {
	queue  []datadogV2.HTTPLogItem
	client *logs.Client
}

// new creates a new DeadLetterQueue object with the given data.
func new(client *logs.Client, queue []datadogV2.HTTPLogItem) *DeadLetterQueue {
	return &DeadLetterQueue{
		client: client,
		queue:  queue,
	}
}

// JSONBytes returns the a []byte representation of the dead letter queue.
func (d *DeadLetterQueue) JSONBytes() ([]byte, error) {
	return json.Marshal(d.queue)
}

// Save saves the dead letter queue to storage
func (d *DeadLetterQueue) Save(ctx context.Context, client *storage.Client) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "deadletterqueue.Client.Save")
	defer span.Finish()
	data, err := d.JSONBytes()
	if err != nil {
		return fmt.Errorf("error marshalling dlq: %w", err)
	}
	err = client.UploadBlob(ctx, storage.ForwarderContainer, BlobName, data)
	if err != nil {
		return fmt.Errorf("uploading dlq failed: %w", err)
	}
	return nil
}

// Process processes the dead letter queue by sending the logs to Datadog.
func (d *DeadLetterQueue) Process(ctx context.Context, logger *log.Entry) error {
	var failedLogs []datadogV2.HTTPLogItem
	for _, datadogLog := range d.queue {
		err := d.client.AddFormattedLog(ctx, logger, datadogLog)
		if err != nil && !errors.Is(err, logs.ErrInvalidLog) {
			failedLogs = append(failedLogs, datadogLog)
		}
	}
	d.queue = failedLogs
	return d.client.Flush(ctx)
}

// Add adds logs to the dead letter queue.
func (d *DeadLetterQueue) Add(logs []datadogV2.HTTPLogItem) {
	d.queue = append(d.queue, logs...)
}

// GetQueue returns the dead letter queue.
func (d *DeadLetterQueue) GetQueue() []datadogV2.HTTPLogItem {
	return d.queue
}

// Load loads the Dead Letter Queue from the storage client.
func Load(ctx context.Context, storageClient *storage.Client, logsClient *logs.Client) (*DeadLetterQueue, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "deadletterqueue.Load")
	defer span.Finish()
	data, err := storageClient.DownloadBlob(ctx, storage.ForwarderContainer, BlobName)
	if err != nil {
		var notFoundError *storage.NotFoundError
		if errors.As(err, &notFoundError) {
			return new(logsClient, nil), nil
		}
		return nil, fmt.Errorf("failed to download dlq: %w", err)
	}
	return FromBytes(logsClient, data)
}

// FromBytes creates a DeadLetterQueue object from the given bytes.
func FromBytes(logsClient *logs.Client, data []byte) (*DeadLetterQueue, error) {
	var datadogLogs []datadogV2.HTTPLogItem
	err := json.Unmarshal(data, &datadogLogs)
	if err != nil {
		return nil, err
	}
	return new(logsClient, datadogLogs), nil
}
