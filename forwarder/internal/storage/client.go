package storage

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type Client struct {
	azBlobClient AzureBlobClient
}

func NewClient(azBlobClient AzureBlobClient) Client {
	return Client{
		azBlobClient: azBlobClient,
	}
}

// AzureBlobClient wraps around the azblob.Client struct, to allow for mocking.
// these are the inherited and used methods.
//
//go:generate mockgen -package=mocks -source=$GOFILE -destination=mocks/mock_$GOFILE
type AzureBlobClient interface {
	NewListContainersPager(o *azblob.ListContainersOptions) *runtime.Pager[azblob.ListContainersResponse]
}

type Iterator[ReturnType any, PagerType any] struct {
	pager  *runtime.Pager[PagerType]
	getter func(PagerType) []ReturnType
}

func (i *Iterator[ReturnType, PagerType]) Next(ctx context.Context) ([]ReturnType, error) {
	if !i.pager.More() {
		return nil, nil
	}

	resp, err := i.pager.NextPage(ctx)
	if err != nil {
		return nil, err
	}

	return i.getter(resp), nil
}

func NewIterator[ReturnType any, PagerType any](pager *runtime.Pager[PagerType], getter func(PagerType) []ReturnType) *Iterator[ReturnType, PagerType] {
	return &Iterator[ReturnType, PagerType]{pager: pager, getter: getter}
}
