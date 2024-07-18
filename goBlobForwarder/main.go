package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/DataDog/azure-log-forwarding-offering/goBlobForwarder/blobStorage"
	"golang.org/x/sync/errgroup"
	_ "golang.org/x/sync/errgroup"
)

type azurePool struct {
	group         *errgroup.Group
	containerChan *chan []byte
}

func runPool() {
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

	err = mainPool.Wait()
	if err != nil {
		log.Println(err)
	}
}

func main() {
	start := time.Now()
	log.Println(fmt.Sprintf("Start time: %v", (time.Now()).String()))
	runPool()
	log.Println(fmt.Sprintf("Run time: %v", time.Since(start).String()))
	log.Println(fmt.Sprintf("Final time: %v", (time.Now()).String()))
}
