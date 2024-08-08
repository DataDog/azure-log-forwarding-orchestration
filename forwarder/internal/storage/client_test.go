package storage_test

import (
	"context"
	"errors"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
)

var pagingError = errors.New("fetched more items than expected")

func newPagingHandler[ContentType any, ResponseType any](items []ContentType, fetcherError error, getter func(ContentType) ResponseType) runtime.PagingHandler[ResponseType] {
	counter := 0
	return runtime.PagingHandler[ResponseType]{
		Fetcher: func(ctx context.Context, response *ResponseType) (ResponseType, error) {
			var containersResponse ResponseType
			if fetcherError != nil {
				return containersResponse, fetcherError
			}
			if len(items) == 0 {
				counter++
				return containersResponse, nil
			}
			if counter >= len(items) {
				return containersResponse, pagingError
			}
			containersResponse = getter(items[counter])
			counter++
			return containersResponse, nil
		},
		More: func(response ResponseType) bool {
			return counter < len(items)
		},
	}
}
