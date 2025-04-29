// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package main

import (
	// stdlib
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"time"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	// datadog
	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/cursor"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/deadletterqueue"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/environment"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/metrics"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	customtime "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/time"
)

// resourceBytes is a struct to hold the resource id and the number of bytes processed for that resource.
type resourceBytes struct {
	resourceId string
	bytes      int64
}

func getLogs(ctx context.Context, storageClient *storage.Client, cursors *cursor.Cursors, blob storage.Blob, piiScrubber logs.Scrubber, logsChannel chan<- *logs.Log) (err error) {
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

	processedRawBytes, processedLogs, err := parseLogs(content.Reader, blob, piiScrubber, logsChannel)

	// linux newlines are 1 byte, but windows newlines are 2
	// if adding another byte per line equals the content length, we have processed a file written by a windows machine.
	// we know we have hit the end and can safely set our cursor to the end of the file.
	if processedRawBytes+processedLogs+cursorOffset == blob.ContentLength {
		processedRawBytes = blob.ContentLength - cursorOffset
	}

	if processedRawBytes+cursorOffset > blob.ContentLength {
		// we have processed more bytes than expected
		// unsafe to save cursor
		return errors.Join(err, fmt.Errorf("processed more bytes than expected for %s", blob.Name))
	}

	// we have processed and submitted logs up to cursorOffset+processedRawBytes whether the error is nil or not
	cursors.Set(blob.Container.Name, blob.Name, cursorOffset+processedRawBytes)

	return err
}

func parseLogs(reader io.ReadCloser, blob storage.Blob, piiScrubber logs.Scrubber, logsChannel chan<- *logs.Log) (int64, int64, error) {
	var processedRawBytes int64
	var processedLogs int64

	var currLog *logs.Log
	var err error
	parsedLogsIter, err := logs.Parse(reader, blob, piiScrubber)
	if err != nil {
		return 0, 0, fmt.Errorf("error parsing logs: %w", err)
	}
	for parsedLog := range parsedLogsIter {
		if parsedLog.Err != nil {
			err = fmt.Errorf("error parsing log: %w", parsedLog.Err)
			break
		}
		currLog = parsedLog.ParsedLog

		processedRawBytes += currLog.RawByteSize
		processedLogs += 1
		logsChannel <- currLog
	}
	return processedRawBytes, processedLogs, err
}

func processLogs(ctx context.Context, logsClient *logs.Client, now customtime.Now, logger *log.Entry, logsCh <-chan *logs.Log, resourceIdCh chan<- string, resourceBytesCh chan<- resourceBytes) (err error) {
	for logItem := range logsCh {
		resourceIdCh <- logItem.ResourceId
		currErr := logsClient.AddLog(ctx, now, logger, logItem)
		err = errors.Join(err, currErr)
		resourceBytesCh <- resourceBytes{resourceId: logItem.ResourceId, bytes: logItem.RawLength()}
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

func writeMetrics(ctx context.Context, storageClient *storage.Client, resourceVolumes map[string]int64, resourceBytes map[string]int64, startTime time.Time, versionTag string) (int, error) {
	metricBlob := metrics.MetricEntry{
		Timestamp:          time.Now().Unix(),
		RuntimeSeconds:     time.Since(startTime).Seconds(),
		ResourceLogVolumes: resourceVolumes,
		ResourceLogBytes:   resourceBytes,
		Version:            versionTag,
	}

	metricBuffer, err := metricBlob.ToBytes()
	if err != nil {
		return 0, fmt.Errorf("error while marshalling metrics: %w", err)
	}

	blobName := metrics.GetMetricFileName(time.Now())

	_ = storageClient.AppendBlob(ctx, storage.ForwarderContainer, blobName, metricBuffer)

	logCount := 0
	for _, v := range resourceVolumes {
		logCount += int(v)
	}

	return logCount, nil
}

func fetchAndProcessLogs(ctx context.Context, storageClient *storage.Client, logsClients []*logs.Client, logger *log.Entry, piiScrubber logs.Scrubber, now customtime.Now, versionTag string) (error, map[string]error) {
	start := now()

	var err error

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
		return err, nil
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
			// TODO (AZINTS-3044): Limit failure modes where we return nil and drop data
			processLogsErr := processLogs(logsCtx, logsClient, now, logger, logCh, resourceIdCh, resourceBytesCh)
			if processLogsErr != nil {
				logger.Warning(fmt.Errorf("error processing logs: %w", processLogsErr))
			}
			return nil
		})
	}

	// Get all the containers
	containers := storageClient.GetLogContainers(ctx, logger)

	currNow := now()
	type blobError struct {
		blob storage.Blob
		err  error
	}
	blobErrorCh := make(chan blobError)

	// Spawn error processing goroutine
	blobErrors := make(map[string]error)
	blobErrorEg, _ := errgroup.WithContext(ctx)
	blobErrorEg.Go(func() error {
		for blobErr := range blobErrorCh {
			currErr := blobErr.err
			if existingErr, ok := blobErrors[blobErr.blob.Name]; ok {
				currErr = errors.Join(currErr, existingErr)
			}
			blobErrors[blobErr.blob.Name] = currErr
		}
		return nil
	})

	// Get all the blobs
	downloadEg, downloadCtx := errgroup.WithContext(ctx)
	downloadEg.SetLimit(channelSize)
	for c := range containers {
		blobs := storageClient.ListBlobs(ctx, c, logger)

		// Per blob spawn goroutine to download and transform
		for blob := range blobs {
			// Skip blobs that are not recent
			// Blobs may have old data that we don't want to process
			if !blob.IsCurrent(currNow) {
				cursors.Delete(blob.Container.Name, blob.Name)
				continue
			}
			downloadEg.Go(func() error {
				downloadErr := getLogs(downloadCtx, storageClient, cursors, blob, piiScrubber, logCh)
				if downloadErr != nil {
					blobErrorCh <- blobError{blob: blob, err: downloadErr}
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
	close(blobErrorCh)
	err = errors.Join(err, blobErrorEg.Wait())

	// Save cursors
	cursorErr := cursors.Save(ctx, storageClient)
	err = errors.Join(err, cursorErr)

	// Write forwarder metrics
	logCount, metricErr := writeMetrics(ctx, storageClient, resourceVolumes, resourceBytesMap, start, versionTag)
	err = errors.Join(err, metricErr)

	logger.Info(fmt.Sprintf("Finished processing %d logs", logCount))
	return err, blobErrors
}

func processDeadLetterQueue(ctx context.Context, now customtime.Now, logger *log.Entry, storageClient *storage.Client, logsClient *logs.Client, flushedLogsClients []*logs.Client) error {
	dlq, err := deadletterqueue.Load(ctx, storageClient, logsClient)
	if err != nil {
		return err
	}

	dlq.Process(ctx, now, logger)

	for _, client := range flushedLogsClients {
		dlq.Add(client.FailedLogs)
	}

	return dlq.Save(ctx, storageClient, now, logger)
}

func run(ctx context.Context, logParent *log.Logger, goroutineCount int, datadogConfig *datadog.Configuration, azBlobClient storage.AzureBlobClient, piiScrubber logs.Scrubber, now customtime.Now, versionTag string) (error, map[string]error) {
	start := time.Now()

	datadogConfig.AddDefaultHeader("dd_evp_origin", "lfo")
	datadogConfig.RetryConfiguration.HTTPRetryTimeout = 90 * time.Second
	datadogClient := datadog.NewAPIClient(datadogConfig)
	datadogLogsClient := datadogV2.NewLogsApi(datadogClient)

	var hookClient *logs.Client
	if environment.Enabled(environment.TelemetryEnabled) {
		hookClient = logs.NewClient(datadogLogsClient)
		hookLogger := log.New()

		logParent.AddHook(logs.NewHook(hookClient, log.NewEntry(hookLogger)))
	}

	logger := logParent.WithField("run_id", start.Unix())
	logger.Info(fmt.Sprintf("Start time: %v", start.String()))

	storageClient := storage.NewClient(azBlobClient)

	var logsClients []*logs.Client
	for range goroutineCount {
		logsClients = append(logsClients, logs.NewClient(datadogLogsClient))
	}

	processErr, blobErrors := fetchAndProcessLogs(ctx, storageClient, logsClients, logger, piiScrubber, now, versionTag)

	dlqErr := processDeadLetterQueue(ctx, now, logger, storageClient, logs.NewClient(datadogLogsClient), logsClients)

	logger.Info(fmt.Sprintf("Run time: %v", time.Since(start).String()))
	logger.Info(fmt.Sprintf("Final time: %v", (time.Now()).String()))

	var flushErr error
	if hookClient != nil {
		flushErr = hookClient.Flush(ctx)
	}

	return errors.Join(processErr, dlqErr, flushErr), blobErrors
}

func parsePiiScrubRules(piiConfigJSON string) (map[string]logs.ScrubberRuleConfig, error) {
	if len(piiConfigJSON) == 0 {
		return map[string]logs.ScrubberRuleConfig{}, nil
	}

	var piiScrubRules map[string]logs.ScrubberRuleConfig
	err := json.Unmarshal([]byte(piiConfigJSON), &piiScrubRules)

	return piiScrubRules, err
}

func main() {
	var err error

	// Set Datadog API Key
	ctx := context.WithValue(
		context.Background(),
		datadog.ContextAPIKeys,
		map[string]datadog.APIKey{
			"apiKeyAuth": {
				Key: environment.Get(environment.DdApiKey),
			},
		},
	)

	// Set Datadog site
	ddSite := environment.Get(environment.DdSite)
	if ddSite == "" {
		ddSite = "datadoghq.com"
	}
	ctx = context.WithValue(ctx,
		datadog.ContextServerVariables,
		map[string]string{
			"site": ddSite,
		})

	log.SetFormatter(&log.JSONFormatter{})
	logger := log.New()

	goroutineString := environment.Get(environment.NumGoroutines)
	if goroutineString == "" {
		goroutineString = "10"
	}
	goroutineCount, err := strconv.Atoi(goroutineString)
	if err != nil {
		logger.Fatal(fmt.Errorf("error parsing %s: %w", environment.NumGoroutines, err).Error())
	}

	// Initialize storage client
	storageAccountConnectionString := environment.Get(environment.AzureWebJobsStorage)
	azBlobClient, err := azblob.NewClientFromConnectionString(storageAccountConnectionString, nil)
	if err != nil {
		logger.Fatal(fmt.Errorf("error creating azure blob client: %w", err).Error())
		return
	}

	piiConfigJSON := environment.Get(environment.PiiScrubberRules)
	piiScrubRules, err := parsePiiScrubRules(piiConfigJSON)
	if err != nil {
		logger.Fatal(fmt.Errorf("error parsing PII scrubber rules: %w", err).Error())
	}

	piiScrubber := logs.NewPiiScrubber(piiScrubRules)

	versionTag := environment.Get(environment.VersionTag)
	if versionTag == "" {
		versionTag = "unknown"
	}

	datadogConfig := datadog.NewConfiguration()
	if environment.Enabled(environment.TelemetryEnabled) {
		servers := datadogConfig.OperationServers["v2.LogsApi.SubmitLog"]
		if len(servers) > 0 {
			server := servers[0]
			site := server.Variables["site"]
			enumValues := site.EnumValues
			if len(enumValues) == 0 || !slices.Contains(enumValues, environment.Get(environment.DdSite)) {
				enumValues = append(enumValues, environment.Get(environment.DdSite))
			}
			site.EnumValues = enumValues
			server.Variables["site"] = site
		}
	}

	err, _ = run(ctx, logger, goroutineCount, datadogConfig, azBlobClient, piiScrubber, time.Now, versionTag)

	if err != nil {
		logger.Fatal(fmt.Errorf("error while running: %w", err).Error())
	}
}
