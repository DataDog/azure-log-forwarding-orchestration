package main

import (
	"bytes"
	"path"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gopkg.in/dnaeon/go-vcr.v3/recorder"
)

func TestRun(t *testing.T) {
	// Integration test for the storage forwarder
	// GIVEN
	rec, err := recorder.New(path.Join("fixtures", "run"))
	assert.NoErrorf(t, err, "failed creating recorder ")
	defer rec.Stop()

	clientOptions := &azblob.ClientOptions{}
	clientOptions.Transport = rec.GetDefaultClient()
	azBlobClient, err := azblob.NewClientWithNoCredential("https://mattlogger.blob.core.windows.net/", clientOptions)
	assert.NoError(t, err)
	client := storage.NewClient(azBlobClient)

	var output []byte
	buffer := bytes.NewBuffer(output)
	logger := log.New()
	logger.SetOutput(buffer)

	// WHEN
	Run(client, log.NewEntry(logger))

	// THEN
	got := string(buffer.Bytes())
	assert.Contains(t, got, "insights-logs-functionapplogs")
}
