package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
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
	segment, err := client.DownloadSegment(ctx, blob, 0)
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
		return nil, fmt.Errorf("mkdir `%s`: %w", path.Dir(filePath), err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(bytes.NewReader(*segment.Content))
	counter := 0
	for scanner.Scan() {
		_, err = f.Write(scanner.Bytes())
		if err != nil {
			return nil, err
		}
		_, err = f.WriteString("\n")
		if err != nil {
			return nil, err
		}
		counter++
		if counter > 50 {
			break
		}
	}

	return segment.Content, nil
}

func generateRunFixtures(ctx context.Context, logger *log.Entry) {
	storageAccountConnectionString := os.Getenv("AzureWebJobsStorage")
	azBlobClient, err := azblob.NewClientFromConnectionString(storageAccountConnectionString, nil)
	if err != nil {
		logger.Fatalf("error creating azure client: %v", err)
	}

	client := storage.NewClient(azBlobClient)

	containers, err := getContainers(ctx, client)
	if err != nil {
		logger.Fatalf("error getting containers: %v", err)
	}

	now := time.Now()

	for _, container := range containers {
		blobs, err := getBlobs(ctx, client, container)
		if err != nil {
			logger.Fatalf("error getting blobs: %v", err)
		}
		for _, blob := range blobs {
			if !storage.Current(blob, now) {
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
	generateRunFixtures(ctx, logger)
}
