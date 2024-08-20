package main

import (
	"bytes"
	"context"
	"path"
	"testing"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/time"

	"gopkg.in/dnaeon/go-vcr.v3/recorder"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestRun(t *testing.T) {
	// Integration test for the storage forwarder
	// GIVEN
	rec, err := recorder.New(path.Join("fixtures", "run"))
	assert.NoError(t, err)
	defer rec.Stop()

	//rec.SetReplayableInteractions(false)

	clientOptions := &azblob.ClientOptions{}
	clientOptions.Transport = rec.GetDefaultClient()
	azBlobClient, err := azblob.NewClientWithNoCredential("https://forwarderintegrationtest.blob.core.windows.net/", clientOptions)
	assert.NoError(t, err)
	client := storage.NewClient(azBlobClient)

	var output []byte
	buffer := bytes.NewBuffer(output)
	logger := log.New()
	logger.SetOutput(buffer)
	span, ctx := tracer.StartSpanFromContext(context.Background(), "forwarder.test")
	defer span.Finish()

	// WHEN
	err = Run(ctx, client, log.NewEntry(logger), time.RecordedNow)

	// THEN
	got := string(buffer.Bytes())
	assert.NoError(t, err)
	assert.Contains(t, got, "insights-logs-functionapplogs")
}
