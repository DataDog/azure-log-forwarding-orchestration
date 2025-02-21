package collections

// FilterMap returns a slice of values converted from type T to type V using the provided
// mapping function. The resulting slice will only contain the values where the mapping
// function returned 'true', allowing items from the input list to be filtered out.
func FilterMap[T any, V any](in []T, f func(T) (V, bool)) []V {
	out := make([]V, 0)
	for _, inVal := range in {
		outVal, ok := f(inVal)
		if ok {
			out = append(out, outVal)
		}
	}
	return out
}
