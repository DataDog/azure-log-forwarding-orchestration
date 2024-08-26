package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"time"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/datadog"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"gopkg.in/dnaeon/go-vcr.v3/cassette"
	"gopkg.in/dnaeon/go-vcr.v3/recorder"
)

func getContainers(ctx context.Context, client *storage.Client) ([]string, error) {
	containerIter := client.GetContainersMatchingPrefix(ctx, "insights-logs-")
	var containers []string
	for {
		containerList, err := containerIter.Next(ctx)

		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, err
		}

		for _, container := range containerList {
			if container == nil {
				continue
			}
			containers = append(containers, *container.Name)
		}
	}
	return containers, nil
}

func getBlobs(ctx context.Context, client *storage.Client, containerName string) ([]storage.Blob, error) {
	blobIter := client.ListBlobs(ctx, containerName)
	var blobs []storage.Blob
	for {
		blobList, err := blobIter.Next(ctx)

		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, err
		}

		for _, blob := range blobList {
			if blob == nil {
				continue
			}
			blobs = append(blobs, storage.Blob{Item: blob, Container: containerName})
		}
	}
	return blobs, nil
}

func getBlobContent(ctx context.Context, client *storage.Client, blob storage.Blob) (*[]byte, error) {
	segment, err := client.DownloadRange(ctx, blob, 0)
	if err != nil {
		return nil, err
	}
	fixturesPath, err := storage.GetAzuriteFixturesPath()
	if err != nil {
		return nil, err
	}
	filePath := path.Join(fixturesPath, *blob.Item.Name)

	err = os.MkdirAll(path.Dir(filePath), 0755)
	if err != nil {
		return nil, err
	}

	var content []byte
	scanner := bufio.NewScanner(bytes.NewReader(*segment.Content))
	counter := 0
	for scanner.Scan() {
		content = append(content, scanner.Bytes()...)
		content = append(content, '\n')
		counter++
		// Ensure we make at least two calls to DD per blob
		if counter > datadog.BufferSize+1 {
			break
		}
	}

	err = os.WriteFile(filePath, content, 0644)
	if err != nil {
		return nil, err
	}

	return segment.Content, nil
}

func deleteCassetteFile(logger *log.Entry, fixturePath string) {
	err := os.Remove(fmt.Sprintf("%s.yaml", fixturePath))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		logger.Fatalf("failed removing fixtures: %v", err)
	}
}

func generateRunFixtures(ctx context.Context, logger *log.Entry, fixturePath string) {
	deleteCassetteFile(logger, fixturePath)

	rec, err := recorder.New(fixturePath)

	if err != nil {
		logger.Fatalf("failed creating recorder: %v", err)
	}
	defer rec.Stop()

	rec.SetReplayableInteractions(false)

	captureHook := func(i *cassette.Interaction) error {
		delete(i.Request.Headers, "Authorization")
		return nil
	}
	rec.AddHook(captureHook, recorder.AfterCaptureHook)

	clientOptions := &azblob.ClientOptions{}
	clientOptions.Transport = rec.GetDefaultClient()
	storageAccountConnectionString := os.Getenv("AzureWebJobsStorage")
	azBlobClient, err := azblob.NewClientFromConnectionString(storageAccountConnectionString, clientOptions)
	if err != nil {
		logger.Fatalf("error creating azure client: %v", err)
	}

	client := storage.NewClient(azBlobClient)

	containers, err := getContainers(ctx, client)
	if err != nil {
		logger.Fatalf("error getting containers: %v", err)
	}

	for _, container := range containers {
		blobs, err := getBlobs(ctx, client, container)
		if err != nil {
			logger.Fatalf("error getting blobs: %v", err)
		}
		for _, blob := range blobs {
			if !storage.Current(blob, time.Now) {
				continue
			}
			logger.Infof("Blob: %s Container: %s", *blob.Item.Name, container)
			_, err := getBlobContent(ctx, client, blob)
			if err != nil {
				logger.Fatalf("error getting blob content for %s: %v", *blob.Item.Name, err)
			}
		}
	}
}

func main() {
	// Integration test fixture recording for the forwarder
	ctx := context.Background()
	logger := log.WithFields(log.Fields{"service": "forwarder"})
	azuriteFixturesPath, err := storage.GetAzuriteFixturesPath()
	if err != nil {
		log.Fatalf("error getting azurite fixtures path: %v", err)
	}
	err = os.RemoveAll(azuriteFixturesPath)
	if err != nil {
		logger.Fatalf("error removing azurite fixtures path: %v", err)
	}
	runFixturePath := path.Join("cmd", "forwarder", "fixtures", "run")
	generateRunFixtures(ctx, logger, runFixturePath)
}
