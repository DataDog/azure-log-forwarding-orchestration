package main

import (
	// stdlib
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/deadletterqueue"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/environment"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/metrics"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	customtime "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/time"
)

// serviceName is the service tag used for APM and logs about this forwarder.
const serviceName = "dd-azure-forwarder"

// resourceBytes is a struct to hold the resource id and the number of bytes processed for that resource.
type resourceBytes struct {
	resourceId string
	bytes      int64
}

func getLogs(ctx context.Context, storageClient *storage.Client, cursors *cursor.Cursors, blob storage.Blob, piiScrubber logs.PiiScrubber, logsChannel chan<- *logs.Log) (err error) {
	cursorOffset := cursors.Get(blob.Container.Name, blob.Name)
	if cursorOffset == blob.ContentLength {
		// Cursor is at the end of the blob, no need to process
		return nil
	}
	if cursorOffset > blob.ContentLength {
		return fmt.Errorf("cursor is ahead of blob length for %s", blob.Name)
	}
	content, err := storageClient.DownloadSegment(ctx, blob, cursorOffset, blob.ContentLength)
	if err != nil {
		return fmt.Errorf("download range for %s: %w", blob.Name, err)
	}

	processedBytes, processedLogs, err := parseLogs(content.Reader, blob.Container.Name, piiScrubber, logsChannel)

	// linux newlines are 1 byte, but windows newlines are 2
	// if adding another byte per line equals the content length, we have processed a file written by a windows machine.
	// we know we have hit the end and can safely set our cursor to the end of the file.
	if processedBytes+processedLogs+cursorOffset == blob.ContentLength {
		processedBytes = blob.ContentLength - cursorOffset
	}

	if processedBytes+cursorOffset > blob.ContentLength {
		// we have processed more bytes than expected
		// unsafe to save cursor
		return errors.Join(err, fmt.Errorf("processed more bytes than expected for %s", blob.Name))
	}

	// we have processed and submitted logs up to cursorOffset+processedBytes whether the error is nil or not
	cursors.Set(blob.Container.Name, blob.Name, cursorOffset+processedBytes)

	return err
}

func parseLogs(reader io.ReadCloser, containerName string, piiScrubber logs.PiiScrubber, logsChannel chan<- *logs.Log) (int64, int64, error) {
	var processedBytes int64
	var processedLogs int64

	var currLog *logs.Log
	var err error
	for currLog, err = range logs.Parse(reader, containerName, piiScrubber) {
		if err != nil {
			break
		}

		processedBytes += currLog.ByteSize
		processedLogs += 1
		logsChannel <- currLog
	}
	return processedBytes, processedLogs, err
}

func processLogs(ctx context.Context, logsClient *logs.Client, logger *log.Entry, logsCh <-chan *logs.Log, resourceIdCh chan<- string, resourceBytesCh chan<- resourceBytes) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "datadog.ProcessLogs")
	defer span.Finish(tracer.WithError(err))
	for logItem := range logsCh {
		resourceIdCh <- logItem.ResourceId
		currErr := logsClient.AddLog(ctx, logger, logItem)
		err = errors.Join(err, currErr)
		resourceBytesCh <- resourceBytes{resourceId: logItem.ResourceId, bytes: int64(logItem.Length())}
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

func getLogBytes(resourceBytesCh <-chan resourceBytes) map[string]int64 {
	var resourceBytesMap = make(map[string]int64)
	for bytes := range resourceBytesCh {
		if _, ok := resourceBytesMap[bytes.resourceId]; !ok {
			resourceBytesMap[bytes.resourceId] = bytes.bytes
			continue
		}
		resourceBytesMap[bytes.resourceId] += bytes.bytes
	}
	return resourceBytesMap
}

func writeMetrics(ctx context.Context, storageClient *storage.Client, resourceVolumes map[string]int64, resourceBytes map[string]int64, startTime time.Time) (int, error) {
	metricBlob := metrics.MetricEntry{
		Timestamp:          time.Now().Unix(),
		RuntimeSeconds:     time.Since(startTime).Seconds(),
		ResourceLogVolumes: resourceVolumes,
		ResourceLogBytes:   resourceBytes,
	}

	metricBuffer, err := metricBlob.ToBytes()

	if err != nil {
		return 0, fmt.Errorf("error while marshalling metrics: %w", err)
	}

	blobName := metrics.GetMetricFileName(time.Now())

	err = storageClient.AppendBlob(ctx, storage.ForwarderContainer, blobName, metricBuffer)

	logCount := 0
	for _, v := range resourceVolumes {
		logCount += int(v)
	}

	return logCount, nil
}

func fetchAndProcessLogs(ctx context.Context, storageClient *storage.Client, logsClients []*logs.Client, logger *log.Entry, piiScrubber logs.PiiScrubber, now customtime.Now) (err error) {
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
	cursors, err := cursor.Load(ctx, storageClient, logger)
	if err != nil {
		return err
	}

	channelSize := len(logsClients)
	var resourceVolumes map[string]int64
	logCh := make(chan *logs.Log, channelSize)
	resourceIdCh := make(chan string, channelSize)
	var resourceBytesMap map[string]int64
	resourceBytesCh := make(chan resourceBytes, channelSize)

	// Spawn log volume processing goroutine
	logVolumeEg, _ := errgroup.WithContext(ctx)
	logVolumeEg.Go(func() error {
		resourceVolumes = getLogVolume(resourceIdCh)
		return nil
	})

	// Spawn log bytes processing goroutine
	logBytesEg, _ := errgroup.WithContext(ctx)
	logBytesEg.Go(func() error {
		resourceBytesMap = getLogBytes(resourceBytesCh)
		return nil
	})

	// Spawn log processing goroutines
	logsEg, logsCtx := errgroup.WithContext(ctx)
	for _, logsClient := range logsClients {
		logsEg.Go(func() error {
			// TODO (AZINTS-2955): Add a dead letter queue to not drop logs when datadog errors
			// TODO (AZINTS-3044): Limit failure modes where we return nil and drop data
			processLogsErr := processLogs(logsCtx, logsClient, logger, logCh, resourceIdCh, resourceBytesCh)
			if processLogsErr != nil {
				logger.Warning(fmt.Errorf("error processing logs: %w", processLogsErr))
			}
			return nil
		})
	}

	// Get all the containers
	containers := storageClient.GetContainersMatchingPrefix(ctx, storage.LogContainerPrefix, logger)

	// Get all the blobs
	currNow := now()
	downloadEg, downloadCtx := errgroup.WithContext(ctx)
	downloadEg.SetLimit(channelSize)
	for c := range containers {
		blobs := storageClient.ListBlobs(ctx, c, logger)

		// Per blob spawn goroutine to download and transform
		for blob := range blobs {
			// Skip blobs that are not recent
			// Blobs may have old data that we don't want to process
			if !blob.IsCurrent(currNow) {
				continue
			}
			downloadEg.Go(func() error {
				downloadErr := getLogs(downloadCtx, storageClient, cursors, blob, piiScrubber, logCh)
				if downloadErr != nil {
					logger.Warning(fmt.Errorf("error processing blob %s from container %s: %w", blob.Name, c.Name, downloadErr))
				}
				return nil
			})
		}
	}

	// Wait for all the goroutines to finish
	err = errors.Join(err, downloadEg.Wait())
	close(logCh)
	err = errors.Join(err, logsEg.Wait())
	close(resourceIdCh)
	err = errors.Join(err, logVolumeEg.Wait())
	close(resourceBytesCh)
	err = errors.Join(err, logBytesEg.Wait())

	// Save cursors
	cursorErr := cursors.Save(ctx, storageClient)
	err = errors.Join(err, cursorErr)

	// Write forwarder metrics
	logCount, metricErr := writeMetrics(ctx, storageClient, resourceVolumes, resourceBytesMap, start)
	err = errors.Join(err, metricErr)

	logger.Info(fmt.Sprintf("Finished processing %d logs", logCount))
	return err
}

func processDeadLetterQueue(ctx context.Context, logger *log.Entry, storageClient *storage.Client, logsClient *logs.Client, flushedLogsClients []*logs.Client) error {
	dlq, err := deadletterqueue.Load(ctx, storageClient, logsClient)
	if err != nil {
		return err
	}

	dlq.Process(ctx, logger)

	for _, client := range flushedLogsClients {
		dlq.Add(client.FailedLogs)
	}

	return dlq.Save(ctx, storageClient, logger)
}

func run(ctx context.Context, logger *log.Entry, goroutineCount int, datadogClient *datadog.APIClient, azBlobClient *azblob.Client, piiScrubber logs.PiiScrubber) error {
	start := time.Now()
	logger.Info(fmt.Sprintf("Start time: %v", start.String()))

	storageClient := storage.NewClient(azBlobClient)

	// Initialize log submission client
	logsApiClient := datadogV2.NewLogsApi(datadogClient)

	var logsClients []*logs.Client
	for range goroutineCount {
		logsClients = append(logsClients, logs.NewClient(logsApiClient))
	}

	processErr := fetchAndProcessLogs(ctx, storageClient, logsClients, logger, piiScrubber, time.Now)

	dlqErr := processDeadLetterQueue(ctx, logger, storageClient, logs.NewClient(logsApiClient), logsClients)

	logger.Info(fmt.Sprintf("Run time: %v", time.Since(start).String()))
	logger.Info(fmt.Sprintf("Final time: %v", (time.Now()).String()))

	return errors.Join(processErr, dlqErr)
}

func main() {
	apmEnabled := environment.APMEnabled()

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
				Key: environment.Get(environment.DD_API_KEY),
			},
		},
	)

	// Set Datadog site
	ddSite := environment.Get(environment.DD_SITE)
	if ddSite == "" {
		ddSite = "datadoghq.com"
	}
	ctx = context.WithValue(ctx,
		datadog.ContextServerVariables,
		map[string]string{
			"site": ddSite,
		})

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
			profiler.WithAPIKey(environment.Get(environment.DD_API_KEY)),
			profiler.WithService(serviceName),
			profiler.WithAgentlessUpload(),
		)
		if err != nil {
			logger.Warning(err)
		}
		defer profiler.Stop()
	}

	forceProfile := environment.Get(environment.DD_FORCE_PROFILE)
	if forceProfile != "" {
		// Sleep for 5 seconds to allow profiler to start
		time.Sleep(5 * time.Second)
	}

	// Initialize Datadog API client
	datadogConfig := datadog.NewConfiguration()
	datadogConfig.RetryConfiguration.HTTPRetryTimeout = 90 * time.Second
	datadogClient := datadog.NewAPIClient(datadogConfig)

	goroutineString := environment.Get(environment.NUM_GOROUTINES)
	if goroutineString == "" {
		goroutineString = "10"
	}
	goroutineCount, err := strconv.ParseInt(goroutineString, 10, 64)
	if err != nil {
		logger.Fatalf(fmt.Errorf("error parsing %s: %w", environment.NUM_GOROUTINES, err).Error())
	}

	// Initialize storage client
	storageAccountConnectionString := environment.Get(environment.AZURE_WEB_JOBS_STORAGE)
	azBlobClient, err := azblob.NewClientFromConnectionString(storageAccountConnectionString, nil)
	if err != nil {
		logger.Fatalf(fmt.Errorf("error creating azure blob client: %w", err).Error())
		return
	}

	piiConfigString := environment.Get(environment.PII_SCRUBBER_RULES)
	var piiScrubRules map[string]logs.ScrubberRuleConfig
	err = json.Unmarshal([]byte(piiConfigString), &piiScrubRules)
	if err != nil {
		fmt.Println("Error unmarshaling JSON:", err)
		return
	}

	piiScrubber := logs.NewPiiScrubber(piiScrubRules)

	err = run(ctx, logger, int(goroutineCount), datadogClient, azBlobClient, piiScrubber)

	if err != nil {
		logger.Fatalf(fmt.Errorf("error while running: %w", err).Error())
	}
}
