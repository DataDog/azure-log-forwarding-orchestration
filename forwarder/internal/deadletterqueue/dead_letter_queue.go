// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

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
	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/collections"
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
func (d *DeadLetterQueue) Save(ctx context.Context, client *storage.Client, logger *log.Entry) error {
	// prune invalid logs
	d.queue = collections.Filter(d.client.FailedLogs, func(log datadogV2.HTTPLogItem) bool {
		_, valid := logs.ValidateDatadogLog(log, logger)
		return valid
	})

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
	var err error
	for _, datadogLog := range d.queue {
		addLogErr := d.client.AddFormattedLog(ctx, logger, datadogLog)
		if addLogErr != nil && !errors.Is(addLogErr, logs.ErrInvalidLog) {
			errors.Join(err, addLogErr)
		}
	}
	flushErr := d.client.Flush(ctx)
	errors.Join(err, flushErr)
	if err != nil {
		logger.Errorf("failed to process dead letter queue: %v", err)
		d.queue = d.client.FailedLogs
	}
}
