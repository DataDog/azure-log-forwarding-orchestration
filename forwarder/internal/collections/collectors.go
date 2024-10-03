package collections

import (
	// stdlib
	"context"
	"errors"
)

func Map[SliceType, ReturnType any](typeSlice []SliceType, fn func(SliceType) ReturnType) []ReturnType {
	result := make([]ReturnType, len(typeSlice))
	for i, t := range typeSlice {
		result[i] = fn(t)
	}
	return result
}

func Collect[ReturnType any, ResponseType any](ctx context.Context, it Iterator[*ReturnType, ResponseType]) ([]*ReturnType, error) {
	var collection []*ReturnType
	for {
		item, err := it.Next(ctx)

		if errors.Is(err, Done) {
			break
		}

		if err != nil {
			return nil, err
		}
		collection = append(collection, item)
	}
	return collection, nil
}
