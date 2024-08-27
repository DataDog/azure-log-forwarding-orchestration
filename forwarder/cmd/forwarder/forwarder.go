package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/metrics"

	dd "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	customtime "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/time"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"

	log "github.com/sirupsen/logrus"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
)

func getContainers(ctx context.Context, client *storage.Client, containerNameCh chan<- string) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "forwarder.getContainers")
	defer span.Finish(tracer.WithError(err))
	defer close(containerNameCh)
	iter := client.GetContainersMatchingPrefix(ctx, storage.LogContainerPrefix)

	for {
		containerList, err := iter.Next(ctx)

		if errors.Is(err, iterator.Done) {
			return nil
		}

		if err != nil {
			return err
		}

		if containerList != nil {
			for _, container := range containerList {
				if container == nil {
					continue
				}
				containerNameCh <- *container.Name
			}
		}
	}
}

func getBlobs(ctx context.Context, client *storage.Client, containerName string, blobChannel chan<- storage.Blob) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "forwarder.getLogsFromBlob")
	defer span.Finish(tracer.WithError(err))
	iter := client.ListBlobs(ctx, containerName)

	for {
		blobList, err := iter.Next(ctx)

		if errors.Is(err, iterator.Done) {
			return nil
		}

		if err != nil {
			return err
		}

		if blobList != nil {
			for _, blob := range blobList {
				if blob == nil {
					continue
				}
				blobChannel <- storage.Blob{Item: blob, Container: containerName}
			}
		}
	}

}

func getBlobContents(ctx context.Context, client *storage.Client, blob storage.Blob, blobContentChannel chan<- storage.BlobSegment) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "forwarder.getBlobContents")
	defer span.Finish(tracer.WithError(err))

	current, downloadErr := client.DownloadRange(ctx, blob, 0)
	if downloadErr != nil {
		return fmt.Errorf("getBlobContents: %v", downloadErr)
	}

	blobContentChannel <- current
	return nil
}

func getLogsFromBlob(ctx context.Context, blob storage.BlobSegment, logsChannel chan<- []byte) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "forwarder.getLogsFromBlob")
	defer span.Finish(tracer.WithError(err))
	scanner := bufio.NewScanner(bytes.NewReader(*blob.Content))
	for scanner.Scan() {
		logsChannel <- []byte(scanner.Text())
	}
	return nil
}

func getMetricFileName(now time.Time) string {
	return now.UTC().Format("2006-01-02-15") + ".json"
}

func Run(ctx context.Context, storageClient *storage.Client, datadogClient *dd.Client, logger *log.Entry, now customtime.Now) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "forwarder.Run")
	defer span.Finish(tracer.WithError(err))

	start := time.Now()

	eg, egCtx := errgroup.WithContext(ctx)

	defer datadogClient.Close(ctx)

	channelSize := 1000

	logCh := make(chan *logs.Log, channelSize)

	resourceVolumes := make(map[string]int64)

	eg.Go(func() error {
		for currLog := range logCh {
			_, ok := resourceVolumes[currLog.ResourceId]
			if !ok {
				resourceVolumes[currLog.ResourceId] = 0
			}
			resourceVolumes[currLog.ResourceId]++
		}
		return nil
	})

	rawLogCh := make(chan []byte, channelSize)

	eg.Go(func() error {
		defer close(logCh)
		for rawLog := range rawLogCh {
			formattedLog, err := logs.NewLog(rawLog)
			if err != nil {
				logger.Error(fmt.Sprintf("Error formatting log: %v", err))
				return err
			}
			logCh <- formattedLog
			err = datadogClient.SubmitLog(egCtx, formattedLog)
			if err != nil {
				logger.Error(fmt.Sprintf("Error submitting log: %v", err))
				return err
			}
		}
		return datadogClient.Flush(egCtx)
	})

	blobContentCh := make(chan storage.BlobSegment, channelSize)

	eg.Go(func() error {
		defer close(rawLogCh)
		for blobContent := range blobContentCh {
			err := getLogsFromBlob(egCtx, blobContent, rawLogCh)
			if err != nil {
				logger.Error(fmt.Sprintf("Error getting logs from blob: %v", err))
				return err
			}
		}
		return nil
	})

	blobCh := make(chan storage.Blob, channelSize)

	eg.Go(func() error {
		defer close(blobContentCh)
		blobsEg, newCtx := errgroup.WithContext(egCtx)
		for blob := range blobCh {
			if !storage.Current(blob, now) {
				continue
			}
			log.Printf("Downloading blob %s", *blob.Item.Name)
			blobsEg.Go(func() error { return getBlobContents(newCtx, storageClient, blob, blobContentCh) })
		}
		return blobsEg.Wait()
	})

	containerNameCh := make(chan string, channelSize)

	eg.Go(func() error {
		defer close(blobCh)
		var err error
		for container := range containerNameCh {
			curErr := getBlobs(egCtx, storageClient, container, blobCh)
			if curErr != nil {
				err = errors.Join(err, curErr)
			}
		}
		return err
	})

	err = getContainers(ctx, storageClient, containerNameCh)

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

	err = storageClient.UploadBlob(ctx, metrics.MetricsBucket, blobName, metricBuffer)

	logger.Info("Finished processing logs")
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
