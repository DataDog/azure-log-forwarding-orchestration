package collections

import (
	// stdlib
	"context"
	"errors"
	"fmt"

	// datadog
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
)

// Done is returned by Iterator's Next method when the iteration is
// complete; when there are no more items to return.
var Done = errors.New("no more items in iterator")

// EmptyPage is returned when an empty list is received from Azure.
var EmptyPage = errors.New("received an empty list from Azure")

// Iterator is a generic iterator for paginated responses containing lists.
type Iterator[ReturnType any, PagerType any] struct {
	azurePager    *runtime.Pager[PagerType]
	internalPager Pager[ReturnType]
	transformer   func(PagerType) []ReturnType
}

// Pager is a generic pager for list responses.
type Pager[ReturnType any] struct {
	page      []ReturnType
	nextIndex int
}

// More returns true if there are more items in the pager.
func (p *Pager[ReturnType]) More() bool {
	return p.nextIndex < len(p.page)
}

// Next returns the next item in the pager.
func (p *Pager[ReturnType]) Next() ReturnType {
	currItem := p.page[p.nextIndex]
	p.nextIndex += 1
	return currItem
}

// NewPager creates a new pager.
func NewPager[ReturnType any](page []ReturnType) (Pager[ReturnType], error) {
	if len(page) == 0 {
		return Pager[ReturnType]{}, EmptyPage
	}
	return Pager[ReturnType]{
		page: page,
	}, nil
}

// Next returns the next item in the iterator.
func (i *Iterator[ReturnType, PagerType]) Next(ctx context.Context) (ReturnType, error) {
	var err error
	if !i.internalPager.More() {
		err = i.getNextPage(ctx)
		if err != nil {
			var zeroValue ReturnType
			return zeroValue, err
		}
	}

	return i.internalPager.Next(), nil
}

func (i *Iterator[ReturnType, PagerType]) getNextPage(ctx context.Context) error {
	var err error
	span, ctx := tracer.StartSpanFromContext(ctx, "storage.Iterator.getNextPage")
	defer span.Finish(tracer.WithError(err))
	if !i.azurePager.More() {
		return Done
	}

	resp, err := i.azurePager.NextPage(ctx)
	if err != nil {
		return fmt.Errorf("getting next page: %w", err)
	}

	page := i.transformer(resp)
	i.internalPager, err = NewPager(page)
	return err
}

// NewIterator creates a new iterator.
func NewIterator[ReturnType any, PagerType any](
	pager *runtime.Pager[PagerType],
	transformer func(PagerType) []ReturnType) Iterator[ReturnType, PagerType] {

	return Iterator[ReturnType, PagerType]{azurePager: pager, transformer: transformer}
}

// PagingError is returned when more items are fetched than expected.
var PagingError = errors.New("fetched more items than expected")

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
				return currResponse, PagingError
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
