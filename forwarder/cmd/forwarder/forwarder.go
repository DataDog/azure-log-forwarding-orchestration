package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

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

type MetricEntry struct {
	Timestamp          int64            `json:"timestamp"`
	RuntimeSeconds     float64          `json:"runtime_seconds"`
	ResourceLogVolumes map[string]int32 `json:"resource_log_volume"`
}

func Run(ctx context.Context, client *storage.Client, datadogClient *dd.Client, logger *log.Entry, now customtime.Now) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "forwarder.Run")
	defer span.Finish(tracer.WithError(err))
	eg, ctx := errgroup.WithContext(ctx)

	defer datadogClient.Close(ctx)

	channelSize := 1000

	rawLogCh := make(chan []byte, channelSize)

	eg.Go(func() error {
		for rawLog := range rawLogCh {
			formattedLog, err := logs.NewLog(rawLog)
			if err != nil {
				logger.Error(fmt.Sprintf("Error formatting log: %v", err))
				return err
			}
			err = datadogClient.SubmitLog(ctx, formattedLog)
			if err != nil {
				logger.Error(fmt.Sprintf("Error submitting log: %v", err))
				return err
			}
		}
		return datadogClient.Flush(ctx)
	})

	blobContentCh := make(chan storage.BlobSegment, channelSize)

	eg.Go(func() error {
		defer close(rawLogCh)
		for blobContent := range blobContentCh {
			err := logs.ParseLogs(*blobContent.Content, rawLogCh)
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
		return storage.GetBlobContents(ctx, logger, client, blobCh, blobContentCh, currNow)
	})

	containerCh := make(chan string, channelSize)

	eg.Go(func() error {
		return storage.GetBlobsPerContainer(ctx, client, blobCh, containerCh)
	})

	err = storage.GetContainers(ctx, client, containerCh)

	err = errors.Join(err, eg.Wait())
	if err != nil {
		return fmt.Errorf("run: %v", err)
	}

	logger.Info("Finished processing logs")

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

	runErr := Run(ctx, storageClient, datadogClient, logger, time.Now)

	resourceVolumeMap := make(map[string]int32)
	//TODO[AZINTS-2653]: Add volume data to resourceVolumeMap once we have it
	metricBlob := MetricEntry{(time.Now()).Unix(), time.Since(start).Seconds(), resourceVolumeMap}

	metricBuffer, err := json.Marshal(metricBlob)

	if err != nil {
		logger.Fatalf("error while marshalling metrics: %v", err)
	}

	dateString := time.Now().UTC().Format("2006-01-02-15")
	blobName := dateString + ".txt"

	err = storageClient.UploadBlob(ctx, "forwarder-metrics", blobName, metricBuffer)

	err = errors.Join(runErr, err)

	logger.Info(fmt.Sprintf("Run time: %v", time.Since(start).String()))
	logger.Info(fmt.Sprintf("Final time: %v", (time.Now()).String()))
	if err != nil {
		logger.Fatalf("error while running: %v", err)
	}
}
