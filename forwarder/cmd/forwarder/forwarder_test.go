package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func uploadBlobs(ctx context.Context, client *storage.Client) error {
	fixturesPath, err := storage.GetAzuriteFixturesPath()
	if err != nil {
		return err
	}
	err = filepath.Walk(fixturesPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || strings.Contains(filePath, ".DS_Store") {
			return nil
		}
		data, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}
		targetPath := filePath[len(fixturesPath):]
		err = client.UploadBlob(ctx, storage.FunctionAppLogsContainerPrefix, targetPath, data)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func TestRun(t *testing.T) {
	// Integration test for the storage forwarder
	// GIVEN
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "mcr.microsoft.com/azure-storage/azurite:latest",
		ExposedPorts: []string{"10000/tcp", "10001/tcp", "10002/tcp"},
		WaitingFor:   wait.ForLog("Azurite Table service is successfully listening at http://0.0.0.0:10002"),
	}
	azurite, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("Could not start azurite: %s", err)
	}
	defer func() {
		if err := azurite.Terminate(ctx); err != nil {
			t.Fatalf("Could not stop azurite: %s", err)
		}
	}()

	blobEndpoint, err := azurite.Endpoint(ctx, "10000/tcp")
	if err != nil {
		t.Fatalf("Could not get azurite blob endpoint: %s", err)
	}
	blobPort := strings.Split(blobEndpoint, ":")[2]

	queueEndpoint, err := azurite.Endpoint(ctx, "10001/tcp")
	if err != nil {
		t.Fatalf("Could not get azurite queue endpoint: %s", err)
	}
	queuePort := strings.Split(queueEndpoint, ":")[2]

	tableEndpoint, err := azurite.Endpoint(ctx, "10002/tcp")
	if err != nil {
		t.Fatalf("Could not get azurite table endpoint: %s", err)
	}
	tablePort := strings.Split(tableEndpoint, ":")[2]

	connectionString := fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:%s/devstoreaccount1;QueueEndpoint=http://127.0.0.1:%s/devstoreaccount1;TableEndpoint=http://127.0.0.1:%s/devstoreaccount1;", blobPort, queuePort, tablePort)

	azBlobClient, err := azblob.NewClientFromConnectionString(connectionString, nil)
	assert.NoError(t, err)

	_, err = azBlobClient.CreateContainer(context.Background(), storage.FunctionAppLogsContainerPrefix, nil)
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "The specified container already exists") {
			err = nil
		}
	}
	assert.NoError(t, err)

	client := storage.NewClient(azBlobClient)

	err = uploadBlobs(context.Background(), client)
	assert.NoError(t, err)

	var output []byte
	buffer := bytes.NewBuffer(output)
	logger := log.New()
	logger.SetOutput(buffer)
	span, ctx := tracer.StartSpanFromContext(context.Background(), "forwarder.test")
	defer span.Finish()

	// WHEN
	err = Run(ctx, client, log.NewEntry(logger), time.Now)

	// THEN
	got := string(buffer.Bytes())
	assert.NoError(t, err)
	assert.Contains(t, got, "Formatted log")
}
