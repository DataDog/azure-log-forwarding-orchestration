package main

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	"golang.org/x/sync/errgroup"
	"log"
	"os"
	"time"
)

func RunWithClient(client *storage.Client) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, ctx = errgroup.WithContext(ctx)

	// Get containers with logs from storage account
	containers, err := client.GetContainersMatchingPrefix(ctx, storage.LogContainerPrefix)
	if err != nil {
		log.Println(fmt.Errorf("error getting contains with prefix %s: %v", storage.LogContainerPrefix, err))
		return
	}
	for _, container := range containers {
		log.Println(fmt.Sprintf("Container: %s", *container))
	}
}

func Run(storageAccountConnectionString string) {
	client, err := storage.NewClient(storageAccountConnectionString, &azblob.ClientOptions{})
	if err != nil {
		log.Println(err)
		return
	}

	RunWithClient(client)
}

func main() {
	start := time.Now()
	log.Println(fmt.Sprintf("Start time: %v", start.String()))
	storageAccountConnectionString := os.Getenv("StorageAccountConnectionString")
	Run(storageAccountConnectionString)
	log.Println(fmt.Sprintf("Run time: %v", time.Since(start).String()))
	log.Println(fmt.Sprintf("Final time: %v", (time.Now()).String()))
}
