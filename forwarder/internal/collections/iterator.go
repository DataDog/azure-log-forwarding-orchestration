package collections

import (
	// stdlib
	"context"
	"errors"
	"iter"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
)

// New creates a new iterator over a sequence generated from an Azure pager.
func New[ReturnType any, PagerType any](
	ctx context.Context,
	pager *runtime.Pager[PagerType],
	transformer func(PagerType) []ReturnType) iter.Seq[ReturnType] {

	return func(yield func(ReturnType) bool) {
		for {
			if !pager.More() {
				return
			}
			resp, err := pager.NextPage(ctx)
			if err != nil {
				return
			}
			for _, item := range transformer(resp) {
				if !yield(item) {
					return
				}
			}
		}
	}
}

// ErrTooManyItems is returned when more items are fetched than expected.
var ErrTooManyItems = errors.New("fetched more items than expected")

// NewPagingHandler creates a new paging handler for usage in tests.
func NewPagingHandler[ContentType any, ResponseType any](
	items []ContentType,
	fetcherError error,
	transformer func(ContentType) ResponseType) runtime.PagingHandler[ResponseType] {

	var idx int
	return runtime.PagingHandler[ResponseType]{
		Fetcher: func(ctx context.Context, response *ResponseType) (ResponseType, error) {
			var currResponse ResponseType
			if fetcherError != nil {
				return currResponse, fetcherError
			}
			if len(items) == 0 {
				idx++
				return currResponse, nil
			}
			if idx >= len(items) {
				return currResponse, ErrTooManyItems
			}
			currResponse = transformer(items[idx])
			idx++
			return currResponse, nil
		},
		More: func(response ResponseType) bool {
			return idx < len(items)
		},
	}
}
