package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"

	log "github.com/sirupsen/logrus"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
)

func Run(spanContext ddtrace.SpanContext, client *storage.Client, logger *log.Entry) error {
	runSpan := tracer.StartSpan("forwarder.Run", tracer.ChildOf(spanContext))
	eg, ctx := errgroup.WithContext(context.Background())

	containerNameCh := make(chan string, 1000)

	eg.Go(func() error {
		for container := range containerNameCh {
			output.Write([]byte(fmt.Sprintf("Container: %s\n", container)))
		}
		return nil
	})

	iter := client.GetContainersMatchingPrefix(storage.LogContainerPrefix, runSpan.Context())

	var err error

	for {
		containerList, err := iter.Next(ctx)

		if errors.Is(err, iterator.Done) {
			err = nil
			break
		}

		if err != nil {
			break
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
	close(containerNameCh)

	err = errors.Join(err, eg.Wait())
	runSpan.Finish(tracer.WithError(err))
	if err != nil {
		return fmt.Errorf("run: %v", err)
	}

	return nil
}

func main() {
	tracer.Start()
	defer tracer.Stop()

	// Start a root span.
	span := tracer.StartSpan("forwarder.main")
	defer span.Finish()

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
		logger.Fatalf("error creating azure client: %v", err)
		return
	}

	client := storage.NewClient(azBlobClient)

	err = Run(span.Context(), client, logger)

	logger.Info(fmt.Sprintf("Run time: %v", time.Since(start).String()))
	logger.Info(fmt.Sprintf("Final time: %v", (time.Now()).String()))
	if err != nil {
		logger.Fatalf("error while running: %v", err)
	}
}
