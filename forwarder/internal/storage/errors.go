package storage

import "fmt"

// NotFoundError is returned when an item is not found
type NotFoundError struct {
	Item string
}

// Error returns what item was not found
func (e *NotFoundError) Error() string {
e.Item + " not found"
}
