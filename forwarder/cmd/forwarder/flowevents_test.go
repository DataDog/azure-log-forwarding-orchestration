// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/cursor"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/environment"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	storagemocks "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage/mocks"
)

func flowRecords(t *testing.T, fixture string) []json.RawMessage {
	t.Helper()
	var doc struct {
		Records []json.RawMessage `json:"records"`
	}
	require.NoError(t, json.Unmarshal([]byte(fixture), &doc))
	return doc.Records
}

func flowDocument(t *testing.T, records []json.RawMessage) []byte {
	t.Helper()
	data, err := json.Marshal(struct {
		Records []json.RawMessage `json:"records"`
	}{records})
	require.NoError(t, err)
	return data
}

func vnetFlowTupleCount(t *testing.T, records []json.RawMessage) int {
	t.Helper()
	var count int
	for _, raw := range records {
		var record struct {
			FlowRecords struct {
				Flows []struct {
					FlowGroups []struct {
						FlowTuples []string `json:"flowTuples"`
					} `json:"flowGroups"`
				} `json:"flows"`
			} `json:"flowRecords"`
		}
		require.NoError(t, json.Unmarshal(raw, &record))
		for _, flow := range record.FlowRecords.Flows {
			for _, group := range flow.FlowGroups {
				count += len(group.FlowTuples)
			}
		}
	}
	return count
}

func flowLogOutputCount(t *testing.T, container string, records []json.RawMessage) int {
	t.Helper()
	if container == logs.VnetFlowEventContainer {
		return vnetFlowTupleCount(t, records)
	}
	return len(records)
}

func drainFlowLogs(out <-chan *logs.Log, count int) {
	for range count {
		<-out
	}
}

func enableVnetFlowTupleSplitting(t *testing.T) {
	t.Helper()
	t.Setenv(environment.SplitVnetFlowTuples, "true")
}

// Exercise getLogs with the real parser and a storage range mock, without Azure
// credentials or an intake connection. Honour the requested range so a test
// fails if the forwarder resumes inside the growing JSON document.
func readFlowSnapshot(t *testing.T, ctx context.Context, data []byte, blob storage.Blob, checkpoints *cursor.Cursors, out chan *logs.Log, expectedDownloads ...int) error {
	t.Helper()
	ctrl := gomock.NewController(t)
	client := storagemocks.NewMockAzureBlobClient(ctrl)
	blob.ContentLength = int64(len(data))
	downloads := 1
	if len(expectedDownloads) > 0 {
		downloads = expectedDownloads[0]
	}
	download := client.EXPECT().DownloadStream(gomock.Any(), blob.Container.Name, blob.Name, gomock.Any()).DoAndReturn(
		func(_ context.Context, _, _ string, options *azblob.DownloadStreamOptions) (azblob.DownloadStreamResponse, error) {
			start := options.Range.Offset
			end := start + options.Range.Count
			response := azblob.DownloadStreamResponse{}
			response.Body = io.NopCloser(bytes.NewReader(data[start:end]))
			return response, nil
		})
	download.Times(downloads)
	return getLogs(ctx, storage.NewClient(client), checkpoints, blob, newMockPiiScrubber(ctrl), out)
}

func TestGrowingFlowLogSnapshots(t *testing.T) {
	enableVnetFlowTupleSplitting(t)
	for _, test := range []struct {
		name, container, fixture string
	}{
		{"VNet", logs.VnetFlowEventContainer, vnetFlowLogData},
		{"NSG", logs.NetworkSecurityGroupFlowEventContainer, networkSecurityGroupFlowEventLogData},
	} {
		t.Run(test.name, func(t *testing.T) {
			records := flowRecords(t, test.fixture)
			checkpoints := cursor.New(nil)
			blob := newBlob("flows", test.container)
			out := make(chan *logs.Log, flowLogOutputCount(t, test.container, records))
			for n := 1; n <= len(records); n++ {
				data := flowDocument(t, records[:n])
				require.NoError(t, readFlowSnapshot(t, context.Background(), data, blob, checkpoints, out))
				expected := flowLogOutputCount(t, test.container, records[n-1:n])
				require.Len(t, out, expected, "each snapshot should emit only its new record")
				drainFlowLogs(out, expected)
				// The same completed snapshot must neither replay a record nor be
				// downloaded again.
				require.NoError(t, readFlowSnapshot(t, context.Background(), data, blob, checkpoints, out, 0))
				require.Empty(t, out)
			}
			// A new hourly blob has an independent checkpoint.
			blob.Name += ".next-hour.json"
			require.NoError(t, readFlowSnapshot(t, context.Background(), flowDocument(t, records[:1]), blob, checkpoints, out))
			require.Len(t, out, flowLogOutputCount(t, test.container, records[:1]))
		})
	}
}

func TestFlowLogCheckpointMigration(t *testing.T) {
	enableVnetFlowTupleSplitting(t)
	records := flowRecords(t, vnetFlowLogData)
	first := flowDocument(t, records[:1])
	blob := newBlob("flows", logs.VnetFlowEventContainer)
	checkpoints := cursor.New(nil)
	// The old implementation saves EOF, including the closing ]} delimiters.
	checkpoints.Set(blob.Container.Name, blob.Name, int64(len(first)))
	out := make(chan *logs.Log, vnetFlowTupleCount(t, records[1:2]))
	require.NoError(t, readFlowSnapshot(t, context.Background(), flowDocument(t, records[:2]), blob, checkpoints, out))
	require.Len(t, out, vnetFlowTupleCount(t, records[1:2]))
	var got map[string]any
	require.NoError(t, json.Unmarshal((<-out).Content, &got))
	var expected map[string]any
	require.NoError(t, json.Unmarshal(records[1], &expected))
	require.Equal(t, expected["time"], got["time"])
	drainFlowLogs(out, vnetFlowTupleCount(t, records[1:2])-1)
}

func TestFlowLogTruncationAndRetry(t *testing.T) {
	enableVnetFlowTupleSplitting(t)
	records := flowRecords(t, vnetFlowLogData)
	full := flowDocument(t, records[:2])
	first := flowDocument(t, records[:1])
	// Simulate a download ending within the second record.
	broken := full[:len(first)+30]
	blob := newBlob("flows", logs.VnetFlowEventContainer)
	checkpoints := cursor.New(nil)
	out := make(chan *logs.Log, vnetFlowTupleCount(t, records))
	require.Error(t, readFlowSnapshot(t, context.Background(), broken, blob, checkpoints, out))
	require.Len(t, out, vnetFlowTupleCount(t, records[:1]))
	drainFlowLogs(out, vnetFlowTupleCount(t, records[:1]))
	require.Equal(t, int64(len(first)-2), checkpoints.Get(blob.Container.Name, blob.Name))
	require.NoError(t, readFlowSnapshot(t, context.Background(), full, blob, checkpoints, out))
	require.Len(t, out, vnetFlowTupleCount(t, records[1:2]), "retry must deliver the previously incomplete record")
}

func TestFlowLogCancellationAndRetry(t *testing.T) {
	enableVnetFlowTupleSplitting(t)
	records := flowRecords(t, vnetFlowLogData)
	data := flowDocument(t, records[:2])
	blob := newBlob("flows", logs.VnetFlowEventContainer)
	checkpoints := cursor.New(nil)
	out := make(chan *logs.Log)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- readFlowSnapshot(t, ctx, data, blob, checkpoints, out) }()
	<-out
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Zero(t, checkpoints.Get(blob.Container.Name, blob.Name), "a partial record must not advance the checkpoint")
	retry := make(chan *logs.Log, vnetFlowTupleCount(t, records[:2]))
	require.NoError(t, readFlowSnapshot(t, context.Background(), data, blob, checkpoints, retry))
	require.Len(t, retry, vnetFlowTupleCount(t, records[:2]), "cancellation must not advance past an unsent tuple")
}

func TestFlowLogMultipleDocuments(t *testing.T) {
	enableVnetFlowTupleSplitting(t)
	records := flowRecords(t, vnetFlowLogData)
	first := flowDocument(t, records[:1])
	second := flowDocument(t, records[1:2])
	data := append(append(first, '\n'), second...)
	blob := newBlob("flows", logs.VnetFlowEventContainer)
	checkpoints := cursor.New(nil)
	out := make(chan *logs.Log, vnetFlowTupleCount(t, records[:2]))
	require.NoError(t, readFlowSnapshot(t, context.Background(), data, blob, checkpoints, out))
	require.Len(t, out, vnetFlowTupleCount(t, records[:2]))
	drainFlowLogs(out, vnetFlowTupleCount(t, records[:2]))
	require.NoError(t, readFlowSnapshot(t, context.Background(), data, blob, checkpoints, out, 0))
	require.Empty(t, out)
}