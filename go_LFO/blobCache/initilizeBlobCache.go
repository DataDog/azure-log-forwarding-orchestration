package blobCache

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"log"
)

func BlobC() {
	// TODO: replace <storage-account-name> with your actual storage account name
	url := "https://ninateststorage.blob.core.windows.net/"
	ctx := context.Background()

	credential, err := azidentity.NewDefaultAzureCredential(nil)
	handleError(err)

	client, err := azblob.NewClient(url, credential, nil)
	handleError(err)

	// Create the container
	containerName := "insights-logs-functionapplogs"
	fmt.Printf("Creating a container named %s\n", containerName)
	// List the blobs in the container
	fmt.Println("Listing the blobs in the container:")

	pager := client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
		Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
	})
	pagerC := client.NewListContainersPager(&azblob.ListContainersOptions{Include: azblob.ListContainersInclude{Metadata: true}})
	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		handleError(err)

		for _, blob := range resp.Segment.BlobItems {
			fmt.Println(*blob.Name)
		}
	}

	for pagerC.More() {
		resp, err := pager.NextPage(context.TODO())
		handleError(err)

		for _, blob := range resp.Segment.BlobItems {
			fmt.Println(*blob.Name)
		}
	}
	_, err = client.CreateContainer(ctx, containerName, nil)
	handleError(err)

}

func handleError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
