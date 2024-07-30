package storage_test

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
)

func newPagingHandler[ContentType any, ResponseType any](items []ContentType, fetcherError error, getter func(ContentType) ResponseType, nilValue ResponseType) runtime.PagingHandler[ResponseType] {
	counter := 0
	return runtime.PagingHandler[ResponseType]{
		Fetcher: func(ctx context.Context, response *ResponseType) (ResponseType, error) {
			var containersResponse ResponseType
			if fetcherError != nil {
				return nilValue, fetcherError
			}
			if len(items) == 0 {
				counter++
				return nilValue, nil
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
