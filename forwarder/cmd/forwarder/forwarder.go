package main

import (
	// stdlib
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"time"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

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

// maxBufferSize is the maximum buffer to use for scanning logs.
// Logs greater than this buffer will be dropped by bufio.Scanner.
// The buffer is defaulted to the maximum value of an integer.
const maxBufferSize = math.MaxInt32

// initialBufferSize is the initial buffer size to use for scanning logs.
const initialBufferSize = 1024 * 1024 * 5

func getLogs(ctx context.Context, storageClient *storage.Client, cursors *cursor.Cursors, blob storage.Blob, logsChannel chan<- *logs.Log) (err error) {
	currentOffset := cursors.GetCursor(blob.Name)
	content, err := storageClient.DownloadSegment(ctx, blob, currentOffset)
	if err != nil {
		return fmt.Errorf("download range for %s: %w", blob.Name, err)
	}

	cursors.SetCursor(blob.Name, blob.ContentLength)

	return parseLogs(content.Reader, logsChannel)
}

func parseLogs(reader io.ReadCloser, logsChannel chan<- *logs.Log) (err error) {
	scanner := bufio.NewScanner(reader)

	// set buffer size so we can process logs bigger than 65kb
	buffer := make([]byte, initialBufferSize)
	scanner.Buffer(buffer, maxBufferSize)

	for scanner.Scan() {
		currLog, currErr := logs.NewLog(scanner.Bytes())
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
		var invalidLogError logs.TooLargeError
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
	containers := storageClient.GetContainersMatchingPrefix(ctx, storage.LogContainerPrefix, logger)

	// Get all the blobs
	currNow := now()
	downloadEg, segmentCtx := errgroup.WithContext(ctx)
	for c := range containers {
		blobs := storageClient.ListBlobs(ctx, c.Name, logger)

		// Per blob spawn goroutine to download and transform
		for blob := range blobs {
			// Skip blobs that are not recent
			// Blobs may have old data that we don't want to process
			if !blob.IsCurrent(currNow) {
				continue
			}
			downloadEg.Go(func() error {
				return getLogs(segmentCtx, storageClient, cursors, blob, logCh)
			})
		}
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
