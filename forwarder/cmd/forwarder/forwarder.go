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

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/metrics"

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

func getMetricFileName(now time.Time) string {
	return now.UTC().Format("2006-01-02-15") + ".json"
}

type metricEntry struct {
	Timestamp          int64            `json:"timestamp"`
	RuntimeSeconds     float64          `json:"runtime_seconds"`
	ResourceLogVolumes map[string]int32 `json:"resource_log_volume"`
}

func getBlobs(ctx context.Context, storageClient *storage.Client, container string) ([]storage.Blob, error) {
	var blobs []storage.Blob
	var err error

	iter := storageClient.ListBlobs(ctx, container)
	for {
		blobList, currErr := iter.Next(ctx)

		if errors.Is(currErr, iterator.Done) {
			break
		}

		if currErr != nil {
			err = errors.Join(fmt.Errorf("getting next page of blobs for %s: %v", container, currErr), err)
		}

		if blobList == nil {
			continue
		}
		for _, b := range blobList {
			if b == nil {
				continue
			}
			blobs = append(blobs, storage.Blob{Item: b, Container: container})
		}
	}
	return blobs, err
}

func getContainers(ctx context.Context, storageClient *storage.Client) ([]string, error) {
	var containers []string
	var err error
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

func getLogs(ctx context.Context, storageClient *storage.Client, blob storage.Blob, logsChannel chan<- *logs.Log) (err error) {
	content, err := storageClient.DownloadSegment(ctx, blob, 0)
	if err != nil {
		return fmt.Errorf("download range for %s: %v", *blob.Item.Name, err)
	}

	return parseLogs(*content.Content, logsChannel)
}

func parseLogs(data []byte, logsChannel chan<- *logs.Log) (err error) {
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

func processLogs(ctx context.Context, logsClient *logs.Client, logsCh <-chan *logs.Log, volumeCh chan<- string) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "datadog.ProcessLogs")
	defer span.Finish(tracer.WithError(err))
	for logItem := range logsCh {
		volumeCh <- logItem.ResourceId
		currErr := logsClient.SubmitLog(ctx, logItem)
		err = errors.Join(err, currErr)
	}
	flushErr := logsClient.Flush(ctx)
	err = errors.Join(err, flushErr)
	return err
}

func getLogVolume(volumeCh <-chan string) map[string]int64 {
	var resourceVolumes = make(map[string]int64)
	for volume := range volumeCh {
		resourceVolumes[volume]++
	}
	return resourceVolumes
}

func writeMetrics(ctx context.Context, storageClient *storage.Client, resourceVolumes map[string]int64, startTime time.Time) (int, error) {
	metricBlob := metrics.MetricEntry{
		Timestamp:          time.Now().Unix(),
		RuntimeSeconds:     time.Since(startTime).Seconds(),
		ResourceLogVolumes: resourceVolumes,
	}

	metricBuffer, err := metricBlob.ToBytes()

	if err != nil {
		return 0, fmt.Errorf("error while marshalling metrics: %v", err)
	}

	blobName := getMetricFileName(time.Now())

	err = storageClient.UploadBlob(ctx, metrics.MetricsContainer, blobName, metricBuffer)

	logCount := 0
	for _, v := range resourceVolumes {
		logCount += int(v)
	}

	return logCount, nil
}

func run(ctx context.Context, storageClient *storage.Client, logsClients []*logs.Client, logger *log.Entry, now customtime.Now) (err error) {
	start := now()

	span, ctx := tracer.StartSpanFromContext(ctx, "forwarder.Run")
	defer func(span ddtrace.Span, err error) {
		span.Finish(tracer.WithError(err))
	}(span, err)

	defer func() {
		for _, logsClient := range logsClients {
			flushErr := logsClient.Flush(ctx)
			if flushErr != nil {
				logger.Error(fmt.Sprintf("Error flushing logs: %v", flushErr))
				err = errors.Join(err, flushErr)
			}
		}
	}()

	channelSize := len(logsClients)
	var resourceVolumes map[string]int64
	logCh := make(chan *logs.Log, channelSize)
	volumeCh := make(chan string, channelSize)

	// Spawn log volume processing goroutine
	logVolumeEg, _ := errgroup.WithContext(ctx)
	logVolumeEg.Go(func() error {
		resourceVolumes = getLogVolume(volumeCh)
		return nil
	})

	// Spawn log processing goroutines
	logsEg, logsCtx := errgroup.WithContext(ctx)
	for _, logsClient := range logsClients {
		logsEg.Go(func() error {
			return processLogs(logsCtx, logsClient, logCh, volumeCh)
		})
	}

	// Get all the containers
	containers, containerErr := getContainers(ctx, storageClient)
	err = errors.Join(err, containerErr)

	// Get all the blobs
	var blobs []storage.Blob
	for _, c := range containers {
		blobsPerContainer, blobsErr := getBlobs(ctx, storageClient, c)
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
			return getLogs(segmentCtx, storageClient, blob, logCh)
		})
	}

	// Wait for all the goroutines to finish
	err = errors.Join(err, downloadEg.Wait())
	close(logCh)
	err = errors.Join(err, logsEg.Wait())
	close(volumeCh)
	err = errors.Join(err, logVolumeEg.Wait())

	// Write forwarder metrics
	logCount, metricErr := writeMetrics(ctx, storageClient, resourceVolumes, start)
	err = errors.Join(err, metricErr)

	logger.Info(fmt.Sprintf("Finished processing %d logs", logCount))
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

	goroutineString := os.Getenv("NUM_GOROUTINES")
	if goroutineString == "" {
		goroutineString = "10"
	}
	goroutineAmount, err := strconv.ParseInt(goroutineString, 10, 64)
	if err != nil {
		logger.Fatalf("error parsing MAX_GOROUTINES: %v", err)
	}

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

	var logsClients []*logs.Client
	for range goroutineAmount {
		logsClients = append(logsClients, logs.NewClient(logsApiClient))
	}

	runErr := run(ctx, storageClient, logsClients, logger, time.Now)

	resourceVolumeMap := make(map[string]int32)
	//TODO[AZINTS-2653]: Add volume data to resourceVolumeMap once we have it
	metricBlob := metricEntry{(time.Now()).Unix(), time.Since(start).Seconds(), resourceVolumeMap}

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
