package main

import (
	"bytes"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"github.com/stretchr/testify/assert"
	"gopkg.in/dnaeon/go-vcr.v3/cassette"
	"gopkg.in/dnaeon/go-vcr.v3/recorder"
	"os"
	"path"
	"testing"
)

func TestRun(t *testing.T) {
	// Integration test for the storage forwarder
	// GIVEN
	rec, err := recorder.New(path.Join("fixtures", "run"))
	assert.NoErrorf(t, err, "failed creating recorder ")
	defer rec.Stop()

	hook := func(i *cassette.Interaction) error {
		delete(i.Request.Headers, "Authorization")
		return nil
	}
	rec.AddHook(hook, recorder.AfterCaptureHook)
	beforeSaveHook := func(i *cassette.Interaction) error {
		i.Response.Uncompressed = true
		return nil
	}
	rec.AddHook(beforeSaveHook, recorder.BeforeSaveHook)

	storageAccountConnectionString := os.Getenv("AzureWebJobsStorage")

	clientOptions := &azblob.ClientOptions{}
	clientOptions.Transport = rec.GetDefaultClient()
	client, err := storage.NewClient(storageAccountConnectionString, clientOptions)
	assert.NoError(t, err)

	var output []byte
	buffer := bytes.NewBuffer(output)

	// WHEN
	Run(client, buffer)

	// THEN
	got := string(buffer.Bytes())
	assert.Contains(t, got, "insights-logs-functionapplogs")
}
