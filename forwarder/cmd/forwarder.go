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

func Run(client *storage.Client, output io.Writer) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, ctx = errgroup.WithContext(ctx)

	// Get containers with logs from storage account
	containers, err := client.GetContainersMatchingPrefix(ctx, storage.LogContainerPrefix)
	if err != nil {
		output.Write([]byte(fmt.Sprintf("error getting contains with prefix %s: %v", storage.LogContainerPrefix, err)))
		return
	}
	for _, container := range containers {
		output.Write([]byte(fmt.Sprintf("Container: %s", *container)))
	}
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
