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

type azurePool struct {
	group         *errgroup.Group
	containerChan *chan []byte
	blobChan      *chan []byte
	LogsChan      *chan []logsProcessing.AzureLogs
}

func runPool() {
	log.Println(fmt.Sprintf("Start time: %v", (time.Now()).String()))
	if logsProcessing.DdApiKey == "" || logsProcessing.DdApiKey == "<DATADOG_API_KEY>" {
		log.Println("You must configure your API key before starting this function (see ## Parameters section)")
		return
	}

	start := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mainPool, ctx := errgroup.WithContext(ctx)

	// Get containers with logs from storage account
	containersPool, err := blobStorage.NewStorageClient(ctx, logsProcessing.StorageAccountConnectionString, nil)
	if err != nil {
		log.Println(fmt.Errorf("error creating containers pool: %v", err))
		return
	}

	mainPool.Go(func() error {
		err := containersPool.GoGetLogContainers()
		if err != nil {
			return fmt.Errorf("error getting container list: %v", err)
		}
		return nil
	})

	// Get logs from blob storage
	blobPool, err := blobStorage.NewStorageClient(ctx, logsProcessing.StorageAccountConnectionString, containersPool.OutChan)
	if err != nil {
		log.Println(fmt.Errorf("error creating blob storage client: %w", err))
		return
	}

	mainPool.Go(func() error {
		err = blobPool.GoGetLogsFromChannelContainer()
		if err != nil {
			return fmt.Errorf("error getting container logs: %w", err)
		}
		return nil
	})

	// Format and batch logs
	processingPool := logsProcessing.NewBlobLogFormatter(ctx, blobPool.OutChan)
	mainPool.Go(func() error {
		err := processingPool.GoFormatAndBatchLogs()
		if err != nil {
			return fmt.Errorf("error processing logs: %w", err)
		}
		return nil
	})

	// Send logs to Datadog
	mainPool.Go(func() error {
		err := logsProcessing.NewDDClient(ctx, processingPool.LogsChan, nil).GoSendWithRetry(start)
		if err != nil {
			return fmt.Errorf("error creating DD client: %v", err)
		}
		return nil
	})

	err = mainPool.Wait()
	if err != nil {
		log.Println(err)
	}
}

func main() {
	start := time.Now()
	log.Println(fmt.Sprintf("Start time: %v", (time.Now()).String()))
	//cursor := client.DownloadBlobCursor()
	runPool()
	log.Println(fmt.Sprintf("Run time: %v", time.Since(start).String()))
	log.Println(fmt.Sprintf("Final time: %v", (time.Now()).String()))
}
