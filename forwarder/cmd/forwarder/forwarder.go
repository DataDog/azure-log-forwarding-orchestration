package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

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

func Run(ctx context.Context, storageClient *storage.Client, logsClient *logs.Client, logger *log.Entry, now customtime.Now) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "forwarder.Run")
	defer span.Finish(tracer.WithError(err))
	start := time.Now()
	eg, egCtx := errgroup.WithContext(ctx)

	defer logsClient.Flush(ctx)

	channelSize := 1000

	logCh := make(chan *logs.Log, channelSize)

	resourceVolumes := make(map[string]int64)

	eg.Go(func() error {
		span, ctx := tracer.StartSpanFromContext(ctx, "datadog.ProcessLogs")
		defer span.Finish(tracer.WithError(err))
		for logItem := range logCh {
			resourceVolumes[logItem.ResourceId]++
			currErr := logsClient.SubmitLog(ctx, logItem)
			err = errors.Join(err, currErr)
		}
		flushErr := logsClient.Flush(ctx)
		err = errors.Join(err, flushErr)
		return err
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
		span, getBlobsCtx := tracer.StartSpanFromContext(egCtx, "Run.GetBlobContents")
		defer span.Finish()
		defer close(blobContentCh)
		blobsEg, segmentCtx := errgroup.WithContext(getBlobsCtx)
		for blob := range blobCh {
			if !storage.Current(blob, currNow) {
				continue
			}
			blobsEg.Go(func() error {
				current, err := storageClient.DownloadSegment(segmentCtx, blob, 0)
				if err != nil {
					return fmt.Errorf("download range for %s: %v", *blob.Item.Name, err)
				}

				blobContentCh <- current
				return nil
			})
		}
		return blobsEg.Wait()
	})

	containerCh := make(chan string, channelSize)

	// Get all the blobs in the containers
	eg.Go(func() error {
		span, blobCtx := tracer.StartSpanFromContext(egCtx, "Run.GetBlobsPerContainer")
		defer span.Finish()
		defer close(blobCh)
		var err error
		for c := range containerCh {
			iter := storageClient.ListBlobs(blobCtx, c)

			for {
				blobList, currErr := iter.Next(blobCtx)

				if errors.Is(currErr, iterator.Done) {
					break
				}

				if currErr != nil {
					err = errors.Join(fmt.Errorf("getting next page of blobs for %s: %v", c, currErr), err)
				}

				if blobList != nil {
					for _, b := range blobList {
						if b == nil {
							continue
						}
						blobCh <- storage.Blob{Item: b, Container: c}
					}
				}
			}
		}
		return err
	})

	// Get all the containers
	containerSpan, containerCtx := tracer.StartSpanFromContext(ctx, "Run.GetAllContainers")
	iter := storageClient.GetContainersMatchingPrefix(containerCtx, storage.LogContainerPrefix)
	for {
		containerList, currErr := iter.Next(containerCtx)

		if errors.Is(currErr, iterator.Done) {
			break
		}

		if err != nil {
			err = errors.Join(fmt.Errorf("getting next page of containers: %v", currErr), err)
			continue
		}

		if containerList != nil {
			for _, container := range containerList {
				if container == nil {
					continue
				}
				containerCh <- *container.Name
			}
		}
	}

	close(containerCh)
	containerSpan.Finish()

	err = errors.Join(err, eg.Wait())

	metricBlob := metrics.MetricEntry{
		Timestamp:          time.Now().Unix(),
		RuntimeSeconds:     time.Since(start).Seconds(),
		ResourceLogVolumes: resourceVolumes,
	}

	metricBuffer, err := metricBlob.ToBytes()

	if err != nil {
		logger.Fatalf("error while marshalling metrics: %v", err)
	}

	blobName := getMetricFileName(time.Now())

	err = storageClient.UploadBlob(ctx, metrics.MetricsContainer, blobName, metricBuffer)

	logCount := 0
	for _, v := range resourceVolumes {
		logCount += int(v)
	}
	logger.Info(fmt.Sprintf("Finished processing %d logs", logCount))
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
		logger.Fatalf("error creating azure blob client: %v", err)
		return
	}

	storageClient := storage.NewClient(azBlobClient)

	datadogConfig := datadog.NewConfiguration()
	datadogConfig.RetryConfiguration.HTTPRetryTimeout = 90 * time.Second
	apiClient := datadog.NewAPIClient(datadogConfig)

	logsClient := datadogV2.NewLogsApi(apiClient)

	datadogClient := logs.NewClient(logsClient)

	err = Run(ctx, storageClient, datadogClient, logger, time.Now)

	logger.Info(fmt.Sprintf("Run time: %v", time.Since(start).String()))
	logger.Info(fmt.Sprintf("Final time: %v", (time.Now()).String()))
	if err != nil {
		logger.Fatalf("error while running: %v", err)
	}
}
