package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"gopkg.in/dnaeon/go-vcr.v3/cassette"
	"gopkg.in/dnaeon/go-vcr.v3/recorder"
	"gopkg.in/yaml.v3"
)

func cleanupBlobs(ctx context.Context, logger *log.Entry) error {
	storageAccountConnectionString := os.Getenv("AzureWebJobsStorage")
	azBlobClient, err := azblob.NewClientFromConnectionString(storageAccountConnectionString, nil)
	if err != nil {
		logger.Fatalf("error creating azure client: %v", err)
	}
	client := storage.NewClient(azBlobClient)
	containers, err := getContainers(ctx, client)
	if err != nil {
		return err
	}
	for _, c := range containers {
		blobs, err := getBlobs(ctx, client, c)
		if err != nil {
			return err
		}
		var latestBlob *storage.Blob
		for _, blob := range blobs {
			if latestBlob == nil {
				latestBlob = &blob
			} else {
				if blob.Item.Properties.LastModified.After(*latestBlob.Item.Properties.LastModified) {
					latestBlob = &blob
				}
			}
			//if err := client.DeleteBlob(ctx, container, blob); err != nil {
			//	return err
			//}
		}
		logger.Printf("saving blob %s", *latestBlob.Item.Name)
		for _, blob := range blobs {
			if blob.Item.Name != latestBlob.Item.Name {
				logger.Printf("deleting blob %s", *blob.Item.Name)
				azBlobClient.DeleteBlob(ctx, c, *blob.Item.Name, nil)
			}
		}
	}
	return nil
}

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
		//if i.Request.Method != "GET" || !strings.Contains(i.Request.URL, ".json") {
		//	return nil
		//}
		//scanner := bufio.NewScanner(bytes.NewReader([]byte(i.Response.Body)))
		//var logs []string
		//needsPopped := true
		//logLimit := 25
		//counter := 0
		//for scanner.Scan() {
		//	logs = append(logs, scanner.Text())
		//	counter += 1
		//	if counter == logLimit {
		//		needsPopped = false
		//		break
		//	}
		//}
		//if needsPopped {
		//	logs = logs[:len(logs)-1]
		//}
		//if err := scanner.Err(); err != nil {
		//	fmt.Fprintln(os.Stderr, "reading standard input:", err)
		//}
		//newBody := ""
		//for i := range logs {
		//	newBody += fmt.Sprintf("%s\n", logs[i])
		//}
		//i.Response.Body = newBody

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
			//if !storage.FromCurrentHour(blob) {
			//	continue
			//}
			logger.Infof("Blob: %s Container: %s", blob, container)
			_, err := getBlobContent(ctx, client, blob)
			if err != nil {
				logger.Fatalf("error getting blob content for %s: %v", blob, err)
			}
		}
	}
}

// cleanRunFixtures will transform the generated fixtures into a human-readable one
func cleanRunFixtures(ctx context.Context, logger *log.Entry, fixturePath string) {
	//yamlFile, err := os.ReadFile(fmt.Sprintf("%s.yaml", fixturePath))
	//if err != nil {
	//	logger.Fatalf("yamlFile.Get err   #%v ", err)
	//}
	//
	//var c cassette.Cassette

	c, err := cassette.Load(fixturePath)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	data, err := yaml.Marshal(c)
	if err != nil {
		log.Fatalf("Marshal: %v", err)
	}

	deleteCassetteFile(logger, fixturePath)

	f, err := os.Create(c.File)
	if err != nil {
		log.Fatalf("Create: %v", err)
	}

	defer f.Close()

	// Honor the YAML structure specification
	// http://www.yaml.org/spec/1.2/spec.html#id2760395
	_, err = f.Write([]byte("---\n"))
	if err != nil {
		log.Fatalf("error adding ---: %v", err)
	}

	_, err = f.Write(data)
	if err != nil {
		log.Fatalf("error writing data: %v", err)
	}
}

func main() {
	// Integration test fixture recording for the forwarder
	ctx := context.Background()
	logger := log.WithFields(log.Fields{"service": "forwarder"})
	runFixturePath := path.Join("cmd", "forwarder", "fixtures", "run")
	cleanupBlobs(ctx, logger)
	generateRunFixtures(ctx, logger, runFixturePath)
	//cleanRunFixtures(ctx, logger, runFixturePath)
}
