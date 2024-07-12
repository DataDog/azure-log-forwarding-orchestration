package main

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"golang.org/x/sync/errgroup"
	"io"
	"log"
	"os"
	"time"
)

func Run(client storage.Client, output io.Writer) error {
	eg, ctx := errgroup.WithContext(context.Background())

	containerListChan := make(chan string, 1000)

	eg.Go(func() error {
		for container := range containerListChan {
			output.Write([]byte(fmt.Sprintf("Container: %s\n", container)))
		}
		return nil
	})

	// Get containers with logs from storage account
	it := client.GetContainersMatchingPrefix(storage.LogContainerPrefix)

	for v, ok, err := it.Next(ctx); ok || err != nil; v, ok, err = it.Next(ctx) {
		if err != nil {
			return fmt.Errorf("error getting next container: %v", err)
		}
		if v == nil {
			continue
		}
		for _, container := range v {
			if container == nil {
				continue
			}
			containerListChan <- *container.Name
		}
	}
	close(containerListChan)

	err := eg.Wait()
	if err != nil {
		return fmt.Errorf("run: %v", err)
	}

	return nil
}

func main() {
	start := time.Now()
	log.Println(fmt.Sprintf("Start time: %v", start.String()))
	storageAccountConnectionString := os.Getenv("AzureWebJobsStorage")
	azBlobClient, err := azblob.NewClientFromConnectionString(storageAccountConnectionString, nil)
	if err != nil {
		log.Fatalf("%v", err)
	}

	client := storage.NewClient(azBlobClient)

	err = Run(client, log.Writer())

	log.Println(fmt.Sprintf("Run time: %v", time.Since(start).String()))
	log.Println(fmt.Sprintf("Final time: %v", (time.Now()).String()))
	if err != nil {
		log.Fatalf("Could not generate new storage client: %v", err)
	}
}
