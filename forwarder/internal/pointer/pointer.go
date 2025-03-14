package pointer

// Get returns a pointer to the value passed in.
func Get[T any](val T) *T {
	return &val
}
