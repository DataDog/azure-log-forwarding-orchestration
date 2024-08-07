package main

import (
	"context"
	"errors"
	"os"
	"path"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"gopkg.in/dnaeon/go-vcr.v3/cassette"
	"gopkg.in/dnaeon/go-vcr.v3/recorder"
)

func getContainers(ctx context.Context, client storage.Client) ([]string, error) {
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

		if containerList != nil {
			for _, container := range containerList {
				if container == nil {
					continue
				}
				containers = append(containers, *container.Name)
			}
		}
	}
	return containers, nil
}

func getBlobs(ctx context.Context, client storage.Client, container string) ([]string, error) {
	blobIter := client.ListBlobs(ctx, container)
	var blobs []string
	for {
		blobList, err := blobIter.Next(ctx)

		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, err
		}

		if blobList != nil {
			for _, blob := range blobList {
				if blob == nil {
					continue
				}
				blobs = append(blobs, *blob.Name)
			}
		}
	}
	return blobs, nil
}

func main() {
	// Integration test fixture recording for the forwarder
	ctx := context.Background()
	logger := log.WithFields(log.Fields{"service": "forwarder"})
	rec, err := recorder.New(path.Join("cmd", "forwarder", "fixtures", "run"))

	if err != nil {
		logger.Fatalf("failed creating recorder: %v", err)
	}
	defer rec.Stop()

	hook := func(i *cassette.Interaction) error {
		delete(i.Request.Headers, "Authorization")
		return nil
	}
	rec.AddHook(hook, recorder.AfterCaptureHook)

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
			logger.Infof("Blob: %s Container: %s", blob, container)
		}
	}

}
