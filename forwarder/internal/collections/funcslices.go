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

// Filter filters slice by condition, keeping any element for which the condition is true.
func Filter[T any, F func(T) bool](slice []T, f F) []T {
	res := make([]T, 0)
	for _, t := range slice {
		if f(t) {
			res = append(res, t)
		}
	}
	return res
}
