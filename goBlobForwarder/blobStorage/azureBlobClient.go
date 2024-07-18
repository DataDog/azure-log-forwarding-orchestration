package blobStorage

import (
	"context"
	"errors"
	"io"
	"log"
)

package blobStorage

import (
"context"
"errors"
"io"
"log"

"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// Rerun if changes are made to the interface
//go:generate mockgen -source=$GOFILE -destination=./tests/mocks/$GOFILE -package=mocks

// AzureBlobClient wraps around the azblob.Client struct, to allow for mocking.
// these are the inherited and used methods.
type AzureBlobClient interface {
	NewListContainersPager(o *azblob.ListContainersOptions) *runtime.Pager[azblob.ListContainersResponse]
}

type Runnable func() error

type ErrGroupInterface interface {
	Go(Runnable)
	Wait() error
	SetLimit(int)
}

type BlobClient struct {
	Client         *azblob.Client
	Context        context.Context
	ContextCancel  context.CancelFunc
	StorageAccount string
}

func NewBlobClient(context context.Context, cancel context.CancelFunc, storageAccount string) (*BlobClient, error) {

	client, err := azblob.NewClientFromConnectionString(storageAccount, nil)
	if err != nil {
		log.Println(err)
		return nil, errors.New("failed to create azure client")
	}

	return &BlobClient{
		Context:        context,
		Client:         client,
		ContextCancel:  cancel,
		StorageAccount: storageAccount,
	}, nil

}

type AzurePager[T any] interface {
	NextPage(ctx context.Context) (T, error)
	More() bool
}
