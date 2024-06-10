package main

import (
	"context"
	"fmt"
	"github.com/DataDog/azure-log-forwarding-offering/go_LFO/LogsProcessing"
	"github.com/DataDog/azure-log-forwarding-offering/go_LFO/blobCache"
	"golang.org/x/sync/errgroup"
	_ "golang.org/x/sync/errgroup"
	"log"
	"os"
	"time"
)

func run(ctx context.Context, data []byte) {
	if LogsProcessing.DdApiKey == "" || LogsProcessing.DdApiKey == "<DATADOG_API_KEY>" {
		log.Println("You must configure your API key before starting this function (see ## Parameters section)")
		return
	}
	inChan := make(chan []byte)
	// format the logs
	handler := LogsProcessing.NewBlobLogFormatter(ctx, inChan)
	// batch logs to avoid sending too many logs in a single request 25KB 4MG
	formatedLogs, _ := handler.BatchBlobData(data)

	// scrub the logs
	scrubberConfig := []LogsProcessing.ScrubberRuleConfigs{
		{
			"REDACT_IP": {
				Pattern:     `[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}`,
				Replacement: "xxx.xxx.nn.xxx",
			},
			"REDACT_EMAIL": {
				Pattern:     `[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+`,
				Replacement: "xx@nn.co",
			},
		},
	}
	// submit logs to Datadog
	LogsProcessing.NewDDClient(context.TODO(), scrubberConfig).SendAll(formatedLogs)
}

type azurePool struct {
	group         *errgroup.Group
	containerChan *chan []byte
	blobChan      chan []byte
	LogsChan      []LogsProcessing.AzureLogs
}

func testLocalFile() {

	data, err := os.ReadFile("<path to your local file>")
	if err != nil {
		log.Fatal(err)
	}

	run(context.Background(), data)
}

func testChannel() {
	start := time.Now()
	mainPool := new(errgroup.Group)

	err, containersPool := blobCache.NewAzureStorageClient(context.Background(), LogsProcessing.StorageAccount, nil)
	if err != nil {
		return
	}
	mainPool.Go(func() error {
		containersPool.GoGetLogContainers()
		return nil
	})

	err, blobPool := blobCache.NewAzureStorageClient(context.Background(), LogsProcessing.StorageAccount, containersPool.OutChan)
	if err != nil {
		return
	}
	mainPool.Go(func() error {
		err = blobPool.GoGetLogsFromChannelContainer()
		return err
	})

	processingPool := LogsProcessing.NewBlobLogFormatter(context.Background(), blobPool.OutChan)
	mainPool.Go(func() error {
		err := processingPool.GoFormatAndBatchLogs()
		return err
	})

	for {
		processedAzureLogs, ok := <-processingPool.LogsChan
		if !ok {
			return
		}
		_ = LogsProcessing.NewDDClient(context.Background(), nil).SendWithRetry(processedAzureLogs)
		// fmt.Println(processedAzureLogs[0].DDRequire)
		fmt.Println(time.Since(start))
	}
}

func main() {
	//testLocalFile()
	start := time.Now()
	testChannel()
	fmt.Sprintf("Final time: %d\n", time.Since(start))
	//cursor := client.DownloadBlobCursor()
	//get logs as byte array
	//parse and send logs
	//run(context.Background(), data)
}
