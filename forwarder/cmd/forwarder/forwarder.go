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
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/environment"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/metrics"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	customtime "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/time"
)

// serviceName is the service tag used for APM and logs about this forwarder.
const serviceName = "dd-azure-forwarder"

// maxBufferSize is the maximum buffer to use for scanning logs.
// Logs greater than this buffer will be dropped by bufio.Scanner.
// The buffer is defaulted to the maximum value of an integer.
const maxBufferSize = math.MaxInt32

// initialBufferSize is the initial buffer size to use for scanning logs.
const initialBufferSize = 1024 * 1024 * 5

// newlineBytes is the number of bytes in a newline character in utf-8.
const newlineBytes = 2

func getLogs(ctx context.Context, storageClient *storage.Client, cursors *cursor.Cursors, blob storage.Blob, logsChannel chan<- *logs.Log) (err error) {
	currentOffset := cursors.GetCursor(blob.Container, blob.Name)
	if currentOffset == blob.ContentLength {
		// Cursor is at the end of the blob, no need to process
		return nil
	}
	if currentOffset > blob.ContentLength {
		return fmt.Errorf("cursor is ahead of blob length for %s", blob.Name)
	}
	content, err := storageClient.DownloadSegment(ctx, blob, currentOffset)
	if err != nil {
		return fmt.Errorf("download range for %s: %w", blob.Name, err)
	}

	writtenBytes, err := parseLogs(content.Reader, logsChannel)

	// we have processed and submitted logs up to currentOffset+int64(writtenBytes) whether the error is nil or not
	cursors.SetCursor(blob.Container, blob.Name, currentOffset+int64(writtenBytes))

	return err
}

func parseLogs(reader io.ReadCloser, logsChannel chan<- *logs.Log) (int, error) {
	var processedBytes int
	scanner := bufio.NewScanner(reader)

	// set buffer size so we can process logs bigger than 65kb
	buffer := make([]byte, initialBufferSize)
	scanner.Buffer(buffer, maxBufferSize)

	for scanner.Scan() {
		currBytes := scanner.Bytes()
		currLog, err := logs.NewLog(currBytes)
		if err != nil {
			if errors.Is(err, logs.ErrIncompleteLog) {
				// azure has not finished writing the file
				// we should stop processing
				break
			}
			return processedBytes, err
		}

		// bufio.Scanner consumes the new line character so we need to add it back
		processedBytes += len(currBytes) + newlineBytes
		logsChannel <- currLog
	}
	return processedBytes, nil
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
	downloadEg.SetLimit(channelSize)
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
	apmEnabled := environment.ApmEnabled()

	if apmEnabled {
		tracer.Start()
		defer tracer.Stop()
	}
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
	logger := log.WithFields(log.Fields{"service": serviceName})

	if apmEnabled {
		err = profiler.Start(
			profiler.WithProfileTypes(
				profiler.CPUProfile,
				profiler.HeapProfile,
				profiler.BlockProfile,
				profiler.MutexProfile,
				profiler.GoroutineProfile,
			),
			profiler.WithAPIKey(os.Getenv("DD_API_KEY")),
			profiler.WithService(serviceName),
			profiler.WithAgentlessUpload(),
		)
		if err != nil {
			logger.Warning(err)
		}
		defer profiler.Stop()
	}

	logger.Info(fmt.Sprintf("Start time: %v", start.String()))

	forceProfile := os.Getenv("DD_FORCE_PROFILE")
	if forceProfile != "" {
		// Sleep for 5 seconds to allow profiler to start
		time.Sleep(5 * time.Second)
	}

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
	metricBlob := metrics.MetricEntry{
		Timestamp:          time.Now().Unix(),
		RuntimeSeconds:     time.Since(start).Seconds(),
		ResourceLogVolumes: resourceVolumeMap,
	}
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
