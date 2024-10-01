package main

import (
	// stdlib
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"

	// datadog
	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/cursor"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/metrics"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	customtime "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/time"
)

// maxBufferSize is the maximum buffer to use for scanning logs
// logs greater than this buffer will be dropped
const maxBufferSize = int(^uint(0) >> 1)

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
			err = errors.Join(fmt.Errorf("getting next page of blobs for %s: %w", container, currErr), err)
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
			err = errors.Join(fmt.Errorf("getting next page of containers: %w", currErr), err)
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

func getLogs(ctx context.Context, storageClient *storage.Client, cursors *cursor.Cursors, blob storage.Blob, logsChannel chan<- *logs.Log) (err error) {
	currentOffset := cursors.GetCursor(*blob.Item.Name)
	content, err := storageClient.DownloadSegment(ctx, blob, currentOffset)
	if err != nil {
		return fmt.Errorf("download range for %s: %w", *blob.Item.Name, err)
	}

	cursors.SetCursor(*blob.Item.Name, *blob.Item.Properties.ContentLength)

	return parseLogs(content.Content, logsChannel)
}

func parseLogs(data []byte, logsChannel chan<- *logs.Log) (err error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))

	// set buffer size so we can process logs bigger than 65kb
	buffer := make([]byte, 0)
	scanner.Buffer(buffer, maxBufferSize)

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

func processLogs(ctx context.Context, logsClient *logs.Client, logger *log.Entry, logsCh <-chan *logs.Log, resourceIdCh chan<- string) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "datadog.ProcessLogs")
	defer span.Finish(tracer.WithError(err))
	for logItem := range logsCh {
		resourceIdCh <- logItem.ResourceId
		currErr := logsClient.AddLog(ctx, logItem)
		var invalidLogError logs.InvalidLogError
		if errors.As(currErr, &invalidLogError) {
			logger.Warning(invalidLogError.Error())
		}
		err = errors.Join(err, currErr)
	}
	flushErr := logsClient.Flush(ctx)
	err = errors.Join(err, flushErr)
	return err
}

func getLogVolume(resourceIdCh <-chan string) map[string]int64 {
	var resourceVolumes = make(map[string]int64)
	for volume := range resourceIdCh {
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
		return 0, fmt.Errorf("error while marshalling metrics: %w", err)
	}

	blobName := metrics.GetMetricFileName(time.Now())

	err = storageClient.UploadBlob(ctx, storage.ForwarderContainer, blobName, metricBuffer)

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
				logger.Error(fmt.Errorf("error flushing logs: %w", flushErr))
				err = errors.Join(err, flushErr)
			}
		}
	}()

	// Download cursors
	cursors, err := cursor.LoadCursors(ctx, storageClient, logger)
	if err != nil {
		return err
	}

	channelSize := len(logsClients)
	var resourceVolumes map[string]int64
	logCh := make(chan *logs.Log, channelSize)
	resourceIdCh := make(chan string, channelSize)

	// Spawn log volume processing goroutine
	logVolumeEg, _ := errgroup.WithContext(ctx)
	logVolumeEg.Go(func() error {
		resourceVolumes = getLogVolume(resourceIdCh)
		return nil
	})

	// Spawn log processing goroutines
	logsEg, logsCtx := errgroup.WithContext(ctx)
	for _, logsClient := range logsClients {
		logsEg.Go(func() error {
			return processLogs(logsCtx, logsClient, logger, logCh, resourceIdCh)
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
			return getLogs(segmentCtx, storageClient, cursors, blob, logCh)
		})
	}

	// Wait for all the goroutines to finish
	err = errors.Join(err, downloadEg.Wait())
	close(logCh)
	err = errors.Join(err, logsEg.Wait())
	close(resourceIdCh)
	err = errors.Join(err, logVolumeEg.Wait())

	// Save cursors
	cursorErr := cursors.SaveCursors(ctx, storageClient)
	err = errors.Join(err, cursorErr)

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

	// Set Datadog API Key
	ctx = context.WithValue(
		ctx,
		datadog.ContextAPIKeys,
		map[string]datadog.APIKey{
			"apiKeyAuth": {
				Key: os.Getenv("DD_API_KEY"),
			},
		},
	)

	// Set Datadog site
	ddSite := os.Getenv("DD_SITE")
	if ddSite == "" {
		ddSite = "datadoghq.com"
	}
	ctx = context.WithValue(ctx,
		datadog.ContextServerVariables,
		map[string]string{
			"site": ddSite,
		})

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
		logger.Fatalf(fmt.Errorf("error parsing MAX_GOROUTINES: %w", err).Error())
	}

	// Initialize storage client
	storageAccountConnectionString := os.Getenv("AzureWebJobsStorage")
	azBlobClient, err := azblob.NewClientFromConnectionString(storageAccountConnectionString, nil)
	if err != nil {
		logger.Fatalf(fmt.Errorf("error creating azure blob client: %w", err).Error())
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

	resourceVolumeMap := make(map[string]int64)
	//TODO[AZINTS-2653]: Add volume data to resourceVolumeMap once we have it
	metricBlob := metrics.MetricEntry{(time.Now()).Unix(), time.Since(start).Seconds(), resourceVolumeMap}
	metricBuffer, err := json.Marshal(metricBlob)

	if err != nil {
		logger.Fatalf(fmt.Errorf("error while marshalling metrics: %w", err).Error())
	}

	blobName := metrics.GetMetricFileName(time.Now())

	err = storageClient.UploadBlob(ctx, storage.ForwarderContainer, blobName, metricBuffer)

	err = errors.Join(runErr, err)

	logger.Info(fmt.Sprintf("Run time: %v", time.Since(start).String()))
	logger.Info(fmt.Sprintf("Final time: %v", (time.Now()).String()))
	if err != nil {
		logger.Fatalf(fmt.Errorf("error while running: %w", err).Error())
	}
}
