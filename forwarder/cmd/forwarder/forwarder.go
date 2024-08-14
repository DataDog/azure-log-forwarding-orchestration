package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"

	log "github.com/sirupsen/logrus"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
)

type MetricEntry struct {
	Timestamp          int64            `json:"timestamp"`
	RuntimeSeconds     float64          `json:"runtime_seconds"`
	ResourceLogVolumes map[string]int32 `json:"resource_log_volume"`
}

func getContainers(ctx context.Context, client storage.Client, containerNameCh chan<- string) error {
	// Get the containers from the storage account
	defer close(containerNameCh)
	iter := client.GetContainersMatchingPrefix(ctx, storage.LogContainerPrefix)

	for {
		containerList, err := iter.Next(ctx)

		if errors.Is(err, iterator.Done) {
			return nil
		}

		if err != nil {
			return err
		}

		if containerList != nil {
			for _, container := range containerList {
				if container == nil {
					continue
				}
				containerNameCh <- *container.Name
			}
		}
	}
}

func getBlobs(ctx context.Context, client storage.Client, containerName string, blobChannel chan<- storage.Blob) error {
	// Get the blobs from the container
	iter := client.ListBlobs(ctx, containerName)

	for {
		blobList, err := iter.Next(ctx)

		if errors.Is(err, iterator.Done) {
			return nil
		}

		if err != nil {
			return err
		}

		if blobList != nil {
			for _, blob := range blobList {
				if blob == nil {
					continue
				}
				blobChannel <- storage.Blob{Name: *blob.Name, Container: containerName}
			}
		}
	}

}

func getBlobContents(ctx context.Context, client storage.Client, containerName string, blobName string, blobContentChannel chan<- storage.BlobSegment) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "forwarder.getBlobContents")
	defer span.Finish(tracer.WithError(err))
	downloadGroup, ctx := errgroup.WithContext(ctx)
	joinGroup, ctx := errgroup.WithContext(ctx)

	initialOffset := 0
	offset := initialOffset
	count := 1024 * 1024 * 25 // 25MB
	//currentUsage := 0
	limit, err := client.GetSize(ctx, containerName, blobName)
	if err != nil {
		return err
	}

	expectedSegments := (limit - initialOffset) / count
	blobSegmentCh := make(chan storage.BlobSegment, expectedSegments)
	//defer close(blobSegmentCh)

	joinGroup.Go(func() error {
		blobBuffer := make([]byte, limit-initialOffset)
		for blobSegment := range blobSegmentCh {
			copy(blobBuffer[blobSegment.Offset:blobSegment.Offset+(blobSegment.Count)], *blobSegment.Content)
		}
		return nil
	})

	joinGroup.Go(func() error {
		for offset < limit {
			if offset+count >= limit {
				count = limit - offset
			}
			downloadGroup.Go(func() error {
				if offset == limit {
					return nil
				}
				current, downloadErr := client.DownloadRange(ctx, containerName, blobName, offset, count)
				if downloadErr != nil {
					return downloadErr
				}
				blobSegmentCh <- current
				return nil
			})
			offset += count
		}

		downloadErr := downloadGroup.Wait()
		close(blobSegmentCh)
		return downloadErr
	})

	err = errors.Join(err, joinGroup.Wait())

	emptyContent := make([]byte, 0)
	blobContentChannel <- storage.BlobSegment{Name: blobName, Container: containerName, Content: &emptyContent, Offset: initialOffset, Count: limit}
	return nil
}

// This function provides a standardized name for each blob that we can use to read and write blobs
// Return type is a string of the current time in the UTC timezone formatted as YYYY-MM-DD-HH
// Standardized with the LogForwarderClient class in log_forwarder_client.py in the control plane
func GetDateTimeString() (date string) {
	return time.Now().UTC().Format("2006-01-02-15")
}

func Run(ctx context.Context, client storage.Client, logger *log.Entry) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "forwarder.Run")
	defer span.Finish(tracer.WithError(err))
	eg, ctx := errgroup.WithContext(context.Background())

	channelSize := 1000

	blobContentCh := make(chan storage.BlobSegment, channelSize)

	eg.Go(func() error {
		for blobContent := range blobContentCh {
			logger.Info(fmt.Sprintf("Downaloded Blob: %s Container: %s, Content: %d", blobContent.Name, blobContent.Container, len(*blobContent.Content)))
		}
		return nil
	})

	blobCh := make(chan storage.Blob, channelSize)

	eg.Go(func() error {
		defer close(blobContentCh)
		var err error
		blobsEg, ctx := errgroup.WithContext(ctx)
		for blob := range blobCh {

			if !storage.FromToday(blob.Name) {
				continue
			}
			log.Printf("Downloading blob %s", blob.Name)
			blobsEg.Go(func() error { return getBlobContents(ctx, client, blob.Container, blob.Name, blobContentCh) })
		}
		err = blobsEg.Wait()
		return err
	})

	containerNameCh := make(chan string, channelSize)

	eg.Go(func() error {
		defer close(blobCh)
		var err error
		for container := range containerNameCh {
			curErr := getBlobs(ctx, client, container, blobCh)
			if curErr != nil {
				err = errors.Join(err, curErr)
			}
		}
		return err
	})

	err = getContainers(ctx, client, containerNameCh)

	err = errors.Join(err, eg.Wait())
	if err != nil {
		return fmt.Errorf("run: %v", err)
	}

	return nil
}

func main() {
	tracer.Start()
	defer tracer.Stop()
	// Start a root span.
	var err error
	span, ctx := tracer.StartSpanFromContext(context.Background(), "forwarder.main")
	defer span.Finish(tracer.WithError(err))

	start := time.Now()
	// use JSONFormatter
	log.SetFormatter(&log.JSONFormatter{})
	logger := log.WithFields(log.Fields{"service": "forwarder"})

	err = profiler.Start(
		profiler.WithProfileTypes(
			profiler.CPUProfile,
			profiler.HeapProfile,
			profiler.BlockProfile,
			profiler.MutexProfile,
			profiler.GoroutineProfile,
		),
		profiler.WithAPIKey(""),
	)
	if err != nil {
		logger.Fatal(err)
	}
	defer profiler.Stop()

	logger.Info(fmt.Sprintf("This is test time: %v", (time.Now()).String()))

	logger.Info(fmt.Sprintf("Start time: %v", start.String()))
	storageAccountConnectionString := os.Getenv("AzureWebJobsStorage")
	azBlobClient, err := azblob.NewClientFromConnectionString(storageAccountConnectionString, nil)
	if err != nil {
		logger.Fatalf("error creating azure client: %v", err)
		return
	}

	client := storage.NewClient(azBlobClient, storageAccountConnectionString)

	runErr := Run(ctx, client, logger)

	resourceVolumeMap := make(map[string]int32)
	//TODO[AZINTS-2653]: Add volume data to resourceVolumeMap once we have it
	metricBlob := MetricEntry{(time.Now()).Unix(), time.Since(start).Seconds(), resourceVolumeMap}

	metricBuffer, err := json.Marshal(metricBlob)

	if err != nil {
		logger.Fatalf("error while marshalling metrics: %v", err)
	}

	dateString := GetDateTimeString()
	blobName := dateString + ".txt"

	err = client.UploadBlob(ctx, "forwarder-metrics", blobName, metricBuffer)

	err = errors.Join(runErr, err)

	logger.Info(fmt.Sprintf("Run time: %v", time.Since(start).String()))
	logger.Info(fmt.Sprintf("Final time: %v", (time.Now()).String()))
	if err != nil {
		logger.Fatalf("error while running: %v", err)
	}
}
