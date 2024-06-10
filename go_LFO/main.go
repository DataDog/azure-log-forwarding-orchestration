package main

import (
	"context"
	"fmt"
	"github.com/DataDog/azure-log-forwarding-offering/go_LFO/LogsProcessing"
	"github.com/DataDog/azure-log-forwarding-offering/go_LFO/blobCache"
	"golang.org/x/sync/errgroup"
	_ "golang.org/x/sync/errgroup"
	"log"
	"time"
)

type azurePool struct {
	group         *errgroup.Group
	containerChan *chan []byte
	blobChan      *chan []byte
	LogsChan      *chan []LogsProcessing.AzureLogs
}

func runPool() {
	if LogsProcessing.DdApiKey == "" || LogsProcessing.DdApiKey == "<DATADOG_API_KEY>" {
		log.Println("You must configure your API key before starting this function (see ## Parameters section)")
		return
	}

	start := time.Now()
	mainPool := new(errgroup.Group)

	// Get containers with logs from storage account
	err, containersPool := blobCache.NewAzureStorageClient(context.Background(), LogsProcessing.StorageAccount, nil)
	if err != nil {
		return
	}
	mainPool.Go(func() error {
		containersPool.GoGetLogContainers()
		return nil
	})

	// Get logs from blob storage
	err, blobPool := blobCache.NewAzureStorageClient(context.Background(), LogsProcessing.StorageAccount, containersPool.OutChan)
	if err != nil {
		return
	}
	mainPool.Go(func() error {
		err = blobPool.GoGetLogsFromChannelContainer()
		return err
	})

	// Format and batch logs
	processingPool := LogsProcessing.NewBlobLogFormatter(context.Background(), blobPool.OutChan)
	mainPool.Go(func() error {
		err := processingPool.GoFormatAndBatchLogs()
		return err
	})

	// Send logs to Datadog
	mainPool.Go(func() error {
		err := LogsProcessing.NewDDClient(context.Background(), processingPool.LogsChan, nil).GoSendWithRetry(start)
		return err
	})

	mainPool.Wait()
}

func main() {
	start := time.Now()
	//cursor := client.DownloadBlobCursor()
	runPool()
	fmt.Println(fmt.Sprintf("Final time: %v", time.Since(start).String()))
}
