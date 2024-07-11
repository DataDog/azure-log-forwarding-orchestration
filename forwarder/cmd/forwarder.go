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

func Run(client *storage.Client, output io.Writer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eg, ctx := errgroup.WithContext(ctx)

	containerListChan := make(chan []*string, 1000)

	// Get containers with logs from storage account
	err := client.GetContainersMatchingPrefix(ctx, eg, storage.LogContainerPrefix, containerListChan)
	if err != nil {
		return fmt.Errorf("error getting contains with prefix %s: %v", storage.LogContainerPrefix, err)
	}
	err = eg.Wait()
	if err != nil {
		return fmt.Errorf("error waiting for errgroup: %v", err)
	}
	select {
	case result := <-containerListChan:
		for _, container := range result {
			output.Write([]byte(fmt.Sprintf("Container: %s", *container)))
		}
	}
	close(containerListChan)
	return nil
}

func main() {
	start := time.Now()
	log.Println(fmt.Sprintf("Start time: %v", start.String()))
	storageAccountConnectionString := os.Getenv("AzureWebJobsStorage")
	client, err := storage.NewClient(storageAccountConnectionString, &azblob.ClientOptions{})
	if err != nil {
		log.Println(err)
		return
	}

	Run(client, log.Writer())
	log.Println(fmt.Sprintf("Run time: %v", time.Since(start).String()))
	log.Println(fmt.Sprintf("Final time: %v", (time.Now()).String()))
}
