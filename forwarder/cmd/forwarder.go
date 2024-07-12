package main

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
	"os"
	"time"
)

func Run(client *storage.Client, logger *log.Entry) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eg, ctx := errgroup.WithContext(ctx)

	containerListChan := make(chan []*string, 1000)
	defer close(containerListChan)

	// Get containers with logs from storage account
	eg.Go(func() error {

		err := client.GetContainersMatchingPrefix(ctx, eg, storage.LogContainerPrefix, containerListChan)
		if err != nil {
			return fmt.Errorf("error getting contains with prefix %s: %v", storage.LogContainerPrefix, err)
		}
		return nil
	})
	eg.Go(func() error {
		select {
		case result := <-containerListChan:
			for _, container := range result {
				logger.Info(fmt.Sprintf("Container: %s", *container))
			}
		}
		return nil
	})

	err := eg.Wait()
	if err != nil {
		return fmt.Errorf("error waiting for errgroup: %v", err)
	}

	return nil
}

func main() {
	tracer.Start()
	defer tracer.Stop()

	start := time.Now()
	// use JSONFormatter
	log.SetFormatter(&log.JSONFormatter{})
	logger := log.WithFields(log.Fields{"service": "forwarder"})

	err := profiler.Start(
		profiler.WithProfileTypes(
			profiler.CPUProfile,
			profiler.HeapProfile,
			profiler.BlockProfile,
			profiler.MutexProfile,
			profiler.GoroutineProfile,
		),
	)
	if err != nil {
		logger.Fatal(err)
	}
	defer profiler.Stop()

	logger.Info(fmt.Sprintf("Start time: %v", start.String()))
	storageAccountConnectionString := os.Getenv("AzureWebJobsStorage")
	client, err := storage.NewClient(storageAccountConnectionString, &azblob.ClientOptions{})
	if err != nil {
		logger.Fatalf("error creating client: %v", err)
		return
	}

	err = Run(client, logger)

	logger.Info(fmt.Sprintf("Run time: %v", time.Since(start).String()))
	logger.Info(fmt.Sprintf("Final time: %v", (time.Now()).String()))
	if err != nil {
		logger.Fatalf("error while running: %v", err)
		return
	}
}
