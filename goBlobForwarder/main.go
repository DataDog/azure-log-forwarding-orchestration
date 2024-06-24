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
	if logsProcessing.DdApiKey == "" || logsProcessing.DdApiKey == "<DATADOG_API_KEY>" {
		log.Println("You must configure your API key before starting this function (see ## Parameters section)")
		return
	}

	start := time.Now()
	ctx, _ := context.WithCancel(context.Background())
	mainPool, ctx := errgroup.WithContext(ctx)

	// Get containers with logs from storage account
	err, containersPool := blobStorage.NewAzureStorageClient(ctx, logsProcessing.StorageAccount, nil)
	if err != nil {
		return
	}
	mainPool.Go(func() error {
		err := containersPool.GoGetLogContainers()
		return err
	})

	// Get logs from blob storage
	err, blobPool := blobStorage.NewAzureStorageClient(ctx, logsProcessing.StorageAccount, containersPool.OutChan)
	if err != nil {
		log.Println(err)
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
		log.Println(err)
		fmt.Println(err)
	}
}

func main() {
	start := time.Now()
	//cursor := client.DownloadBlobCursor()
	runPool()
	fmt.Println(fmt.Sprintf("Final time: %v", time.Since(start).String()))
}
