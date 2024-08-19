package main

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"path"
	"testing"

	"gopkg.in/dnaeon/go-vcr.v3/cassette"
	"gopkg.in/dnaeon/go-vcr.v3/recorder"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type Transport struct {
	rec *cassette.Cassette
}

func (t *Transport) Do(r *http.Request) (*http.Response, error) {
	if err := r.Context().Err(); err != nil {
		return nil, err
	}

	i, err := t.rec.GetInteraction(r)
	if err != nil {
		return nil, err
	}
	resp := &http.Response{
		StatusCode: i.Response.Code,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte(i.Response.Body))),
		Header:     i.Response.Headers,
	}
	return resp, nil
}

func TestRun(t *testing.T) {
	// Integration test for the storage forwarder
	// GIVEN
	rec, err := recorder.New(path.Join("fixtures", "run"))
	assert.NoError(t, err)
	defer rec.Stop()

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
	err = Run(ctx, client, log.NewEntry(logger))

	// THEN
	got := string(buffer.Bytes())
	assert.NoError(t, err)
	assert.Contains(t, got, "insights-logs-functionapplogs")
}
