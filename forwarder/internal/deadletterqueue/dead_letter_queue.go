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

// Load loads the DeadLetterQueue from the storage client.
func Load(ctx context.Context, storageClient *storage.Client, logsClient *logs.Client) (*DeadLetterQueue, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "deadletterqueue.Load")
	defer span.Finish()
	data, err := storageClient.DownloadBlob(ctx, storage.ForwarderContainer, BlobName)
	if err != nil {
		var notFoundError *storage.NotFoundError
		if errors.As(err, &notFoundError) {
			return newDeadLetterQueue(logsClient, nil), nil
		}
		return nil, fmt.Errorf("failed to download dead letter queue: %w", err)
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
	return newDeadLetterQueue(logsClient, datadogLogs), nil
}

// new creates a new DeadLetterQueue object with the given data.
func newDeadLetterQueue(client *logs.Client, queue []datadogV2.HTTPLogItem) *DeadLetterQueue {
	return &DeadLetterQueue{
		client: client,
		queue:  queue,
	}
}

// GetQueue returns the DeadLetterQueue.
func (d *DeadLetterQueue) GetQueue() []datadogV2.HTTPLogItem {
	return d.queue
}

// JSONBytes returns the a []byte representation of the DeadLetterQueue.
func (d *DeadLetterQueue) JSONBytes() ([]byte, error) {
	return json.Marshal(d.queue)
}

// Add adds logs to the DeadLetterQueue.
func (d *DeadLetterQueue) Add(logs []datadogV2.HTTPLogItem) {
	d.queue = append(d.queue, logs...)
}

// Save saves the DeadLetterQueue to storage
func (d *DeadLetterQueue) Save(ctx context.Context, client *storage.Client) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "deadletterqueue.Client.Save")
	defer span.Finish()
	data, err := d.JSONBytes()
	if err != nil {
		return fmt.Errorf("unable to marshall dead letter queue: %w", err)
	}
	err = client.UploadBlob(ctx, storage.ForwarderContainer, BlobName, data)
	if err != nil {
		return fmt.Errorf("uploading dead letter queue failed: %w", err)
	}
	return nil
}

// Process processes the DeadLetterQueue by sending the logs to Datadog.
func (d *DeadLetterQueue) Process(ctx context.Context, logger *log.Entry) {
	var failedLogs []datadogV2.HTTPLogItem
	for _, datadogLog := range d.queue {
		err := d.client.AddFormattedLog(ctx, logger, datadogLog)
		if err != nil && !errors.Is(err, logs.ErrInvalidLog) {
			failedLogs = append(failedLogs, datadogLog)
		}
	}
	d.queue = failedLogs
	err := d.client.Flush(ctx)
	if err != nil {
		d.queue = append(d.queue, d.client.FailedLogs...)
	}
}
