package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/cursor"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/metrics"

	dd "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/datadog"
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

func Run(ctx context.Context, storageClient *storage.Client, datadogClient *dd.Client, logger *log.Entry, now customtime.Now) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "forwarder.Run")
	defer span.Finish(tracer.WithError(err))

	start := time.Now()

	eg, egCtx := errgroup.WithContext(ctx)

	defer datadogClient.Close(ctx)

	cursors, err := cursor.LoadCursors(ctx, storageClient)
	if err != nil {
		return fmt.Errorf("error getting cursors: %v", err)
	}

	channelSize := 1000

	logCh := make(chan *logs.Log, channelSize)

	resourceVolumes := make(map[string]int64)

	eg.Go(func() error {
		logsErr := dd.ProcessLogs(egCtx, datadogClient, resourceVolumes, logCh)
		return logsErr
	})

	blobContentCh := make(chan storage.BlobSegment, channelSize)

	eg.Go(func() error {
		defer close(logCh)
		for blobContent := range blobContentCh {
			err := logs.ParseLogs(*blobContent.Content, logCh)
			if err != nil {
				logger.Error(fmt.Sprintf("Error getting logs from blob: %v", err))
				return err
			}
		}
		return nil
	})

	blobCh := make(chan storage.Blob, channelSize)
	currNow := now()

	eg.Go(func() error {
		return storage.GetBlobContents(egCtx, logger, storageClient, blobCh, blobContentCh, currNow, cursors)
	})

	containerCh := make(chan string, channelSize)

	eg.Go(func() error {
		return storage.GetBlobsPerContainer(egCtx, storageClient, blobCh, containerCh)
	})

	err = storage.GetContainers(ctx, storageClient, containerCh)

	err = errors.Join(err, eg.Wait())

	cursorErr := cursors.SaveCursors(ctx, storageClient)
	err = errors.Join(err, cursorErr)

	metricBlob := metrics.MetricEntry{
		Timestamp:          time.Now().Unix(),
		RuntimeSeconds:     time.Since(start).Seconds(),
		ResourceLogVolumes: resourceVolumes,
	}

	metricBuffer, marshalError := metricBlob.ToBytes()

	if marshalError != nil {
		logger.Fatalf("error while marshalling metrics: %v", marshalError)
	}

	blobName := getMetricFileName(time.Now())

	uploadErr := storageClient.AppendBlob(ctx, metrics.MetricsBucket, blobName, metricBuffer)
	err = errors.Join(err, uploadErr)

	totalLogs := 0
	for _, v := range resourceVolumes {
		totalLogs += int(v)
	}

	logger.Info(fmt.Sprintf("Finished processing %d logs", totalLogs))
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
	ctx = context.WithValue(
		ctx,
		datadog.ContextAPIKeys,
		map[string]datadog.APIKey{
			"apiKeyAuth": {
				Key: os.Getenv("DD_API_KEY"),
			},
			"appKeyAuth": {
				Key: os.Getenv("DD_APP_KEY"),
			},
		},
	)
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

	logger.Info(fmt.Sprintf("Start time: %v", start.String()))
	storageAccountConnectionString := os.Getenv("AzureWebJobsStorage")
	azBlobClient, err := azblob.NewClientFromConnectionString(storageAccountConnectionString, nil)
	if err != nil {
		logger.Fatalf("error creating azure storageClient: %v", err)
		return
	}

	storageClient := storage.NewClient(azBlobClient)

	datadogConfig := datadog.NewConfiguration()
	apiClient := datadog.NewAPIClient(datadogConfig)

	logsClient := datadogV2.NewLogsApi(apiClient)

	datadogClient := dd.NewClient(logsClient)

	err = Run(ctx, storageClient, datadogClient, logger, time.Now)

	logger.Info(fmt.Sprintf("Run time: %v", time.Since(start).String()))
	logger.Info(fmt.Sprintf("Final time: %v", (time.Now()).String()))
	if err != nil {
		logger.Fatalf("error while running: %v", err)
	}
}
