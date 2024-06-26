package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/DataDog/azure-log-forwarding-offering/goBlobForwarder/blobStorage"
	"github.com/DataDog/azure-log-forwarding-offering/goBlobForwarder/logsProcessing"
	"golang.org/x/sync/errgroup"
	_ "golang.org/x/sync/errgroup"
)

func runPool() {
	if logsProcessing.DdApiKey == "" || logsProcessing.DdApiKey == "<DATADOG_API_KEY>" {
		log.Println("You must configure your API key before starting this function (see ## Parameters section)")
		return
	}

	start := time.Now()
	ctx, _ := context.WithCancel(context.Background())
	mainPool, ctx := errgroup.WithContext(ctx)

	// Get containers with logs from storage account
	containersPool, err := blobStorage.NewStorageClient(ctx, logsProcessing.StorageAccountConnectionString, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	mainPool.Go(func() error {
		err := containersPool.GoGetLogContainers()
		return err
	})

	// Get logs from blob storage inside a container
	blobPool, err := blobStorage.NewStorageClient(ctx, logsProcessing.StorageAccountConnectionString, containersPool.OutChan)
	if err != nil {
		fmt.Println(err)
		return
	}
	mainPool.Go(func() error {
		err = blobPool.GoGetLogsFromChannelContainer()
		return err
	})

	// Format and batch logs
	processingPool := logsProcessing.NewBlobLogFormatter(ctx, blobPool.OutChan)
	mainPool.Go(func() error {
		err := processingPool.GoFormatAndBatchLogs()
		return err
	})

	// Send logs to Datadog
	mainPool.Go(func() error {
		err := logsProcessing.NewDDClient(ctx, processingPool.LogsChan, nil).GoSendWithRetry(start)
		return err
	})

	err = mainPool.Wait()
	if err != nil {
		fmt.Println(err)
	}
}

func main() {
	start := time.Now()
	// TODO: cursor := client.DownloadBlobCursor()
	runPool()
	fmt.Println(fmt.Sprintf("Final time: %v", time.Since(start).String()))
}
