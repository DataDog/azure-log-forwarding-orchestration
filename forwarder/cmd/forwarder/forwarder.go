package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"google.golang.org/api/iterator"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	customtime "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/time"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
)

type MetricEntry struct {
	Timestamp          int64            `json:"timestamp"`
	RuntimeSeconds     float64          `json:"runtime_seconds"`
	ResourceLogVolumes map[string]int32 `json:"resource_log_volume"`
}

func GetBlobs(ctx context.Context, client *storage.Client, container string) (blobs []storage.Blob, err error) {
	iter := client.ListBlobs(ctx, container)

	for {
		blobList, currErr := iter.Next(ctx)

		if errors.Is(currErr, iterator.Done) {
			break
		}

		if currErr != nil {
			err = errors.Join(fmt.Errorf("getting next page of blobs for %s: %v", container, currErr), err)
		}

		if blobList != nil {
			for _, b := range blobList {
				if b == nil {
					continue
				}
				blobs = append(blobs, storage.Blob{Item: b, Container: container})
			}
		}
	}
	return blobs, err
}

func GetContainers(ctx context.Context, storageClient *storage.Client) (containers []string, err error) {
	iter := storageClient.GetContainersMatchingPrefix(ctx, storage.LogContainerPrefix)
	for {
		containerList, currErr := iter.Next(ctx)

		if errors.Is(currErr, iterator.Done) {
			break
		}

		if currErr != nil {
			err = errors.Join(fmt.Errorf("getting next page of containers: %v", currErr), err)
			continue
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
	return containers, err
}

func GetLogs(ctx context.Context, client *storage.Client, blob storage.Blob, logsChannel chan<- *logs.Log) (err error) {
	content, err := client.DownloadSegment(ctx, blob, 0)
	if err != nil {
		return fmt.Errorf("download range for %s: %v", *blob.Item.Name, err)
	}

	return ParseLogs(*content.Content, logsChannel)
}

func ParseLogs(data []byte, logsChannel chan<- *logs.Log) (err error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		currLog, currErr := logs.NewLog([]byte(scanner.Text()))
		if currErr != nil {
			err = errors.Join(err, currErr)
			continue
		}

		logsChannel <- currLog
	}
	return err
}

func ProcessLogs(ctx context.Context, logsClient *logs.Client, logsCh <-chan *logs.Log) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "datadog.ProcessLogs")
	defer span.Finish(tracer.WithError(err))
	for logItem := range logsCh {
		currErr := logsClient.SubmitLog(ctx, logItem)
		err = errors.Join(err, currErr)
	}
	flushErr := logsClient.Flush(ctx)
	err = errors.Join(err, flushErr)
	return err
}

func Run(ctx context.Context, client *storage.Client, logsClient *logs.Client, logger *log.Entry, now customtime.Now, goroutineCount int) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "forwarder.Run")
	defer span.Finish(tracer.WithError(err))

	defer func() {
		flushErr := logsClient.Flush(ctx)
		if flushErr != nil {
			logger.Error(fmt.Sprintf("Error flushing logs: %v", flushErr))
			err = errors.Join(err, flushErr)
		}
	}()

	channelSize := goroutineCount * 10

	logCh := make(chan *logs.Log, channelSize)

	// Spawn log processing goroutines
	logsEg, logsCtx := errgroup.WithContext(ctx)
	for range goroutineCount {
		logsEg.Go(func() error {
			return ProcessLogs(logsCtx, logsClient, logCh)
		})
	}

	// Get all the containers
	containers, containerErr := GetContainers(ctx, client)
	err = errors.Join(err, containerErr)

	// Get all the blobs
	var blobs []storage.Blob
	for _, c := range containers {
		blobsPerContainer, blobsErr := GetBlobs(ctx, client, c)
		err = errors.Join(err, blobsErr)
		blobs = append(blobs, blobsPerContainer...)
	}

	// Per blob spawn goroutine to download and transform
	currNow := now()
	downloadEg, segmentCtx := errgroup.WithContext(ctx)
	for _, blob := range blobs {
		// Skip blobs that are not recent
		// Blobs may have old data that we don't want to process
		if !storage.Current(blob, currNow) {
			continue
		}
		downloadEg.Go(func() error {
			return GetLogs(segmentCtx, client, blob, logCh)
		})
	}
	err = errors.Join(err, downloadEg.Wait())
	close(logCh)

	err = errors.Join(err, logsEg.Wait())
	logger.Info("Finished processing logs")
	return err
}

func main() {
	tracer.Start()
	defer tracer.Stop()
	var err error
	span, ctx := tracer.StartSpanFromContext(context.Background(), "forwarder.main")
	defer span.Finish(tracer.WithError(err))
	ctx = context.WithValue(
		ctx,
		datadog.ContextAPIKeys,
		map[string]datadog.APIKey{
			"apiKeyAuth": {
				Key: os.Getenv("DD_API_KEY"),
			},
		},
	)
	start := time.Now()
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
		logger.Warning(err)
	}
	defer profiler.Stop()

	logger.Info(fmt.Sprintf("Start time: %v", start.String()))

	// Initialize storage client
	storageAccountConnectionString := os.Getenv("AzureWebJobsStorage")
	azBlobClient, err := azblob.NewClientFromConnectionString(storageAccountConnectionString, nil)
	if err != nil {
		logger.Fatalf("error creating azure blob client: %v", err)
		return
	}
	storageClient := storage.NewClient(azBlobClient)

	// Initialize log submission client
	datadogConfig := datadog.NewConfiguration()
	datadogConfig.RetryConfiguration.HTTPRetryTimeout = 90 * time.Second
	apiClient := datadog.NewAPIClient(datadogConfig)
	logsApiClient := datadogV2.NewLogsApi(apiClient)
	logsClient := logs.NewClient(logsApiClient)

	goroutineString := os.Getenv("NUM_GOROUTINES")
	if goroutineString == "" {
		goroutineString = "10"
	}

	goroutineAmount, err := strconv.ParseInt(goroutineString, 10, 64)
	if err != nil {
		logger.Fatalf("error parsing MAX_GOROUTINES: %v", err)
	}

	runErr := Run(ctx, storageClient, logsClient, logger, time.Now, int(goroutineAmount))

	resourceVolumeMap := make(map[string]int32)
	//TODO[AZINTS-2653]: Add volume data to resourceVolumeMap once we have it
	metricBlob := MetricEntry{(time.Now()).Unix(), time.Since(start).Seconds(), resourceVolumeMap}

	metricBuffer, err := json.Marshal(metricBlob)

	if err != nil {
		logger.Fatalf("error while marshalling metrics: %v", err)
	}

	dateString := time.Now().UTC().Format("2006-01-02-15")
	blobName := dateString + ".txt"

	err = storageClient.UploadBlob(ctx, storage.ForwarderContainer, blobName, metricBuffer)

	err = errors.Join(runErr, err)

	logger.Info(fmt.Sprintf("Run time: %v", time.Since(start).String()))
	logger.Info(fmt.Sprintf("Final time: %v", (time.Now()).String()))
	if err != nil {
		logger.Fatalf("error while running: %v", err)
	}
}
